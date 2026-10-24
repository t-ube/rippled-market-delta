#pragma once

// Shadow-only local stream for committed open-ledger state changes.
// Disabled unless XRPLD_MARKET_DELTA_SOCKET is set. No socket I/O or JSON
// serialization is done on the ledger application thread.
#include <xrpl/json/json_value.h>
#include <xrpl/json/to_string.h>
#include <xrpl/ledger/OpenView.h>
#include <xrpl/protocol/LedgerFormats.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <memory>
#include <mutex>
#include <stop_token>
#include <string>
#include <thread>
#include <utility>

#if defined(__linux__)
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>

#include <poll.h>
#include <unistd.h>

#include <cerrno>
#endif

namespace xrpl {

class MarketDeltaTap final
{
public:
    using Snapshot = std::shared_ptr<OpenView const>;

    // Intentionally constructed only if explicitly enabled. The node behaves
    // exactly as upstream if the variable is unset.
    static MarketDeltaTap*
    instance() noexcept
    {
#if defined(__linux__)
        static std::unique_ptr<MarketDeltaTap> tap = [] {
            auto const* path = std::getenv("XRPLD_MARKET_DELTA_SOCKET");
            if (!path || !*path)
                return std::unique_ptr<MarketDeltaTap>{};
            try
            {
                return std::unique_ptr<MarketDeltaTap>(new MarketDeltaTap(path));
            }
            catch (...)
            {
                // Even failing to start the observer must never stop xrpld.
                std::fprintf(stderr, "market-delta: optional observer unavailable\\n");
                return std::unique_ptr<MarketDeltaTap>{};
            }
        }();
        return tap.get();
#else
        return nullptr;
#endif
    }

    ~MarketDeltaTap()
    {
        worker_.request_stop();
        ready_.notify_all();
    }

    // Called while OpenLedger holds its modify lock. Only a bounded queue
    // operation is permitted here; worker owns all diff calculation and I/O.
    void
    modified(Snapshot before, Snapshot after) noexcept
    {
        enqueue(Kind::Delta, std::move(before), std::move(after));
    }

    void
    reset(Snapshot after) noexcept
    {
        enqueue(Kind::Reset, nullptr, std::move(after));
    }

private:
    enum class Kind { Delta, Reset };
    struct Event
    {
        Kind kind;
        std::uint64_t revision;
        Snapshot before;
        Snapshot after;
        std::chrono::steady_clock::time_point queued;
        std::uint64_t firstRevision;
        std::uint64_t coalesced = 0;
    };

    // Process normal events in order, but absorb ledger bursts on the worker.
    // The hard cap bounds retained snapshots if the observer stalls.
    static constexpr std::size_t maxQueued_ = 1024;
    static constexpr std::size_t coalesceThreshold_ = 64;
    std::string const path_;
    std::mutex mutex_;
    std::condition_variable ready_;
    std::deque<Event> pending_;
    std::atomic<bool> connected_{false};
    std::atomic<bool> disconnect_{false};
    std::uint64_t revision_ = 0;
    std::jthread worker_;

    explicit MarketDeltaTap(std::string path)
        : path_(std::move(path)), worker_([this](std::stop_token stop) {
            try
            {
                run(stop);
            }
            catch (...)
            {
                connected_.store(false, std::memory_order_release);
                std::fprintf(stderr, "market-delta: observer worker stopped unexpectedly\\n");
            }
        })
    {
    }

    void
    enqueue(Kind kind, Snapshot before, Snapshot after) noexcept
    {
        if (!connected_.load(std::memory_order_acquire) ||
            disconnect_.load(std::memory_order_acquire))
            return;
        try
        {
            std::lock_guard const lock(mutex_);
            auto const now = std::chrono::steady_clock::now();
            auto const revision = ++revision_;
            if (pending_.size() >= maxQueued_)
            {
                // The worker owns snapshot destruction. Never clear a queue
                // of large OpenViews on the ledger application thread.
                disconnect_.store(true, std::memory_order_release);
                ready_.notify_one();
                return;
            }
            pending_.push_back({kind, revision, std::move(before), std::move(after), now, revision});
            ready_.notify_one();
        }
        catch (...)
        {
            // Allocation errors must not propagate into the consensus path.
            // Force a disconnect so the next client has to resynchronize.
            disconnect_.store(true, std::memory_order_release);
            ready_.notify_one();
        }
    }

    static bool
    marketEntry(SLE::const_pointer const& p)
    {
        if (!p)
            return false;
        switch (p->getType())
        {
            case ltOFFER:
            case ltAMM:
            case ltACCOUNT_ROOT:
            case ltRIPPLE_STATE:
                return true;
            default:
                return false;
        }
    }

    static std::string
    encode(Event const& event)
    {
        json::Value out(json::ValueType::Object);
        out["schema"] = 1;
        out["revision"] = std::to_string(event.revision);
        out["kind"] = event.kind == Kind::Reset ? "ledger_reset" : "market_delta";
        if (event.coalesced != 0)
        {
            out["revisionFrom"] = std::to_string(event.firstRevision);
            out["coalesced"] = std::to_string(event.coalesced);
        }
        if (event.after)
        {
            out["openLedger"] = static_cast<unsigned>(event.after->seq());
            out["parentHash"] = to_string(event.after->header().parentHash);
        }
        out["queueUs"] = std::to_string(
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - event.queued)
                .count());

        if (event.kind == Kind::Delta && event.before && event.after)
        {
            json::Value changes(json::ValueType::Array);
            event.after->visitChangesSince(
                *event.before,
                [&](uint256 const& key, SLE::const_pointer before, SLE::const_pointer after) {
                    if (!marketEntry(before) && !marketEntry(after))
                        return;
                    json::Value change(json::ValueType::Object);
                    change["index"] = to_string(key);
                    change["action"] = !before ? "created" : !after ? "deleted" : "modified";
                    change["before"] = before ? before->getJson() : json::Value{};
                    change["after"] = after ? after->getJson() : json::Value{};
                    changes.append(std::move(change));
                });
            out["changes"] = std::move(changes);
        }
        return json::to_string(out) + "\n";
    }

#if defined(__linux__)
    // AF_UNIX SOCK_STREAM can make partial writes, even without EAGAIN.
    // Bound send latency so a slow reader never stalls this observer indefinitely.
    static bool
    writeAllOrDisconnect(int fd, std::string const& line) noexcept
    {
        auto const deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(100);
        std::size_t sent = 0;
        while (sent < line.size())
        {
            auto const n = ::send(
                fd, line.data() + sent, line.size() - sent, MSG_NOSIGNAL | MSG_DONTWAIT);
            if (n > 0)
            {
                sent += static_cast<std::size_t>(n);
                continue;
            }
            if (n == 0)
                return false;
            if (errno == EINTR)
                continue;
            if (errno != EAGAIN && errno != EWOULDBLOCK)
                return false;
            auto const remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
                deadline - std::chrono::steady_clock::now());
            if (remaining.count() <= 0)
                return false;
            auto const timeout = static_cast<int>(
                remaining.count() > 20 ? 20 : remaining.count());
            pollfd pfd{fd, POLLOUT, 0};
            auto const ready = ::poll(&pfd, 1, timeout);
            if (ready < 0 && errno == EINTR)
                continue;
            if (ready < 0 || (pfd.revents & (POLLERR | POLLHUP | POLLNVAL)))
                return false;
        }
        return true;
    }

    void
    run(std::stop_token stop)
    {
        sockaddr_un addr{};
        if (path_.size() >= sizeof(addr.sun_path))
        {
            std::fprintf(stderr, "market-delta: socket path too long\n");
            return;
        }

        int const server = ::socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        if (server < 0)
            return;
        std::memcpy(addr.sun_path, path_.c_str(), path_.size() + 1);
        addr.sun_family = AF_UNIX;

        // Refuse to replace a regular file or someone else's socket.
        struct stat existing{};
        if (::lstat(path_.c_str(), &existing) == 0)
        {
            if (!S_ISSOCK(existing.st_mode) || existing.st_uid != ::geteuid())
            {
                ::close(server);
                return;
            }
            // Never unlink the socket of another running xrpld instance.
            int const probe = ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
            if (probe < 0)
            {
                ::close(server);
                return;
            }
            int const connected =
                ::connect(probe, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
            int const connectErrno = errno;
            ::close(probe);
            if (connected == 0 || connectErrno != ECONNREFUSED || ::unlink(path_.c_str()) != 0)
            {
                ::close(server);
                return;
            }
        }
        else if (errno != ENOENT)
        {
            ::close(server);
            return;
        }

        if (::bind(server, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0 ||
            ::chmod(path_.c_str(), S_IRUSR | S_IWUSR) != 0 || ::listen(server, 1) != 0)
        {
            std::fprintf(stderr, "market-delta: unable to bind local socket\n");
            ::close(server);
            return;
        }

        int client = -1;
        while (!stop.stop_requested())
        {
            if (client < 0)
            {
                connected_.store(false, std::memory_order_release);
                pollfd pfd{server, POLLIN, 0};
                if (::poll(&pfd, 1, 50) <= 0)
                    continue;
                client = ::accept4(server, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
                if (client < 0)
                    continue;
                {
                    std::lock_guard const lock(mutex_);
                    pending_.clear();
                    disconnect_.store(false, std::memory_order_release);
                }
                // No snapshot barrier exists yet. Clients MUST keep shadow
                // mode and fetch a synchronized snapshot before trusting data.
                if (!writeAllOrDisconnect(
                        client,
                        "{\"schema\":1,\"kind\":\"resync_required\",\"reason\":\"new_subscriber\"}"
                        "\n"))
                {
                    ::close(client);
                    client = -1;
                    continue;
                }
                connected_.store(true, std::memory_order_release);
                continue;
            }

            Event event{};
            {
                std::unique_lock lock(mutex_);
                ready_.wait_for(lock, std::chrono::milliseconds(50), [&] {
                    return !pending_.empty() || disconnect_.load(std::memory_order_acquire) ||
                        stop.stop_requested();
                });
                if (disconnect_.load(std::memory_order_acquire))
                {
                    pending_.clear();
                    lock.unlock();
                    (void)writeAllOrDisconnect(
                        client,
                        "{\"schema\":1,\"kind\":\"resync_required\","
                        "\"reason\":\"queue_overflow_or_error\"}\n");
                    ::close(client);
                    client = -1;
                    continue;
                }
                if (pending_.empty())
                    continue;
                event = std::move(pending_.front());
                pending_.pop_front();
                // Consolidate only consecutive updates of the same open-ledger epoch.
                // Never merge across a ledger reset; keep first/last snapshots.
                if (event.kind == Kind::Delta && pending_.size() >= coalesceThreshold_)
                {
                    while (!pending_.empty() && pending_.front().kind == Kind::Delta &&
                        pending_.front().after && event.after &&
                        pending_.front().after->seq() == event.after->seq() &&
                        pending_.front().after->header().parentHash ==
                            event.after->header().parentHash)
                    {
                        auto tail = std::move(pending_.front());
                        pending_.pop_front();
                        event.revision = tail.revision;
                        event.after = std::move(tail.after);
                        event.coalesced += tail.coalesced + 1;
                    }
                }
            }

            try
            {
                auto const frame = encode(event);
                if (frame.size() > 16 * 1024 * 1024 ||
                    !writeAllOrDisconnect(client, frame))
                {
                    std::fprintf(stderr, "market-delta: frame too large or send failed; resync required\n");
                    ::close(client);
                    client = -1;
                }
            }
            catch (...)
            {
                // A failure to build the delta invalidates continuity.
                if (!writeAllOrDisconnect(
                        client,
                        "{\"schema\":1,\"kind\":\"resync_required\",\"reason\":\"encode_error\"}"
                        "\n"))
                {
                    ::close(client);
                    client = -1;
                }
                else
                {
                    ::close(client);
                    client = -1;
                }
            }
        }

        connected_.store(false, std::memory_order_release);
        if (client >= 0)
            ::close(client);
        ::close(server);
        ::unlink(path_.c_str());
    }
#else
    void
    run(std::stop_token)
    {
    }
#endif
};

}  // namespace xrpl
