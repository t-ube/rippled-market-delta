#pragma once

// Shadow-only local stream for committed open-ledger state changes.
// Disabled unless XRPLD_MARKET_DELTA_SOCKET is set. No socket I/O or JSON
// serialization is done on the ledger application thread.
#include <xrpl/json/json_reader.h>
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
#include <set>
#include <vector>
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
    Snapshot latest_;  // Protected with pending_ and revision_ by mutex_.
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
        try
        {
            std::lock_guard const lock(mutex_);
            // Capture the latest immutable published view even without a
            // subscriber, so the subscription boundary is always available.
            latest_ = after;
            if (!connected_.load(std::memory_order_relaxed) ||
                disconnect_.load(std::memory_order_relaxed))
                return;
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
            pending_.push_back(
                {kind, revision, std::move(before), std::move(after), now, revision});
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
            auto const n =
                ::send(fd, line.data() + sent, line.size() - sent, MSG_NOSIGNAL | MSG_DONTWAIT);
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
            auto const timeout = static_cast<int>(remaining.count() > 20 ? 20 : remaining.count());
            pollfd pfd{fd, POLLOUT, 0};
            auto const ready = ::poll(&pfd, 1, timeout);
            if (ready < 0 && errno == EINTR)
                continue;
            if (ready < 0 || (pfd.revents & (POLLERR | POLLHUP | POLLNVAL)))
                return false;
        }
        return true;
    }


    static std::string
    assetKey(json::Value const& amount)
    {
        if (amount.isString())
            return "XRP";
        if (!amount.isObject() || !amount["currency"].isString())
            return {};
        auto const currency = amount["currency"].asString();
        if (currency == "XRP")
            return "XRP";
        if (!amount["issuer"].isString())
            return {};
        return currency + "." + amount["issuer"].asString();
    }

    static std::string
    pairKey(std::string const& a, std::string const& b)
    {
        if (a.empty() || b.empty() || a == b)
            return {};
        return a < b ? a + "|" + b : b + "|" + a;
    }

    struct Subscription
    {
        bool requested = false;
        std::set<std::string> pairs;
    };

    static Subscription
    readSubscription(int client)
    {
        Subscription sub;
        pollfd pfd{client, POLLIN, 0};
        if (::poll(&pfd, 1, 250) <= 0 || !(pfd.revents & POLLIN))
            return sub;  // Legacy, receive-only subscriber.
        sub.requested = true;
        std::string line;
        line.reserve(8192);
        char block[4096];
        while (line.size() <= 256 * 1024)
        {
            auto const n = ::recv(client, block, sizeof(block), MSG_DONTWAIT);
            if (n > 0)
            {
                line.append(block, static_cast<std::size_t>(n));
                if (line.find('\n') != std::string::npos)
                    break;
            }
            else if (n < 0 && errno == EINTR)
                continue;
            else if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
            {
                pollfd p{client, POLLIN, 0};
                if (::poll(&p, 1, 250) <= 0)
                    break;
            }
            else
                break;
        }
        auto const eol = line.find('\n');
        if (eol == std::string::npos || line.size() > 256 * 1024)
            return sub;
        json::Reader reader;
        json::Value request;
        if (!reader.parse(line.substr(0, eol), request) || !request.isObject() ||
            !request["command"].isString() ||
            request["command"].asString() != "snapshot" ||
            !request["pairs"].isArray() || request["pairs"].size() == 0 ||
            request["pairs"].size() > 4096)
            return sub;
        for (auto const& p : request["pairs"])
        {
            if (!p.isString() || p.asString().size() > 120 ||
                p.asString().find('|') == std::string::npos)
            {
                sub.pairs.clear();
                return sub;
            }
            sub.pairs.insert(p.asString());
        }
        return sub;
    }

    // Send a complete, immutable initial market view before ANY queued
    // updates. The view/revision capture and enqueue share a single mutex.
    // Bounded chunks ensure no oversized NDJSON frames; a failed snapshot
    // invalidates the client rather than silently creating a partial view.
    static bool
    writeSnapshot(
        int client,
        Snapshot const& view,
        std::uint64_t revision,
        std::set<std::string> const& pairs)
    {
        if (!view || pairs.empty())
            return false;
        json::Value begin(json::ValueType::Object);
        begin["schema"] = 1;
        begin["kind"] = "snapshot_begin";
        begin["revision"] = std::to_string(revision);
        begin["openLedger"] = static_cast<unsigned>(view->seq());
        begin["parentHash"] = to_string(view->header().parentHash);
        begin["pairCount"] = static_cast<unsigned>(pairs.size());
        if (!writeAllOrDisconnect(client, json::to_string(begin) + "\n"))
            return false;

        json::Value entries(json::ValueType::Array);
        std::set<std::string> owners;
        std::set<std::string> assets;
        std::size_t count = 0;
        std::size_t bufferedBytes = 0;
        for (auto const& pair : pairs)
        {
            auto const sep = pair.find('|');
            assets.insert(pair.substr(0, sep));
            assets.insert(pair.substr(sep + 1));
        }
        auto flush = [&] {
            if (entries.size() == 0)
                return true;
            json::Value part(json::ValueType::Object);
            part["schema"] = 1;
            part["kind"] = "snapshot_chunk";
            part["revision"] = std::to_string(revision);
            part["entries"] = std::move(entries);
            entries = json::Value(json::ValueType::Array);
            bufferedBytes = 0;
            return writeAllOrDisconnect(client, json::to_string(part) + "\n");
        };
        auto add = [&](SLE::const_pointer const& sle, json::Value const& data) {
            json::Value item(json::ValueType::Object);
            item["index"] = to_string(sle->key());
            item["after"] = data;
            auto const bytes = json::to_string(item).size();
            if (bufferedBytes + bytes > 128 * 1024 && !flush())
                return false;
            bufferedBytes += bytes;
            entries.append(std::move(item));
            ++count;
            return true;
        };
        // First pass: all Offers and AMMs for the tracked pairs; no book
        // depth truncation, including all inherited validated-ledger entries.
        for (auto const& sle : view->sles)
        {
            if (!sle || (sle->getType() != ltOFFER && sle->getType() != ltAMM))
                continue;
            auto const data = sle->getJson();
            auto const pair = sle->getType() == ltOFFER
                ? pairKey(assetKey(data["TakerPays"]), assetKey(data["TakerGets"]))
                : pairKey(assetKey(data["Asset"]), assetKey(data["Asset2"]));
            if (!pairs.contains(pair))
                continue;
            if (data["Account"].isString())
                owners.insert(data["Account"].asString());
            if (!add(sle, data))
                return false;
        }
        // Include issuer settings (e.g. TransferRate) and all seller/AMM
        // account balances required to price funds in this same revision.
        for (auto const& asset : assets)
        {
            auto const sep = asset.find('.');
            if (sep != std::string::npos)
                owners.insert(asset.substr(sep + 1));
        }
        for (auto const& sle : view->sles)
        {
            if (!sle || (sle->getType() != ltACCOUNT_ROOT &&
                         sle->getType() != ltRIPPLE_STATE))
                continue;
            auto const data = sle->getJson();
            if (sle->getType() == ltACCOUNT_ROOT)
            {
                if (!data["Account"].isString() ||
                    !owners.contains(data["Account"].asString()))
                    continue;
            }
            else
            {
                auto const& low = data["LowLimit"];
                auto const& high = data["HighLimit"];
                if (!low.isObject() || !high.isObject() ||
                    !low["issuer"].isString() || !high["issuer"].isString() ||
                    !low["currency"].isString())
                    continue;
                auto const l = low["issuer"].asString();
                auto const h = high["issuer"].asString();
                auto const cur = low["currency"].asString();
                if (!owners.contains(l) && !owners.contains(h))
                    continue;
                if (!assets.contains(cur + "." + l) &&
                    !assets.contains(cur + "." + h))
                    continue;
            }
            if (!add(sle, data))
                return false;
        }
        if (!flush())
            return false;
        json::Value end(json::ValueType::Object);
        end["schema"] = 1;
        end["kind"] = "snapshot_end";
        end["revision"] = std::to_string(revision);
        end["openLedger"] = static_cast<unsigned>(view->seq());
        end["parentHash"] = to_string(view->header().parentHash);
        end["entryCount"] = static_cast<unsigned>(count);
        return writeAllOrDisconnect(client, json::to_string(end) + "\n");
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
                auto const subscription = readSubscription(client);
                Snapshot initial;
                std::uint64_t initialRevision = 0;
                {
                    std::lock_guard const lock(mutex_);
                    pending_.clear();
                    disconnect_.store(false, std::memory_order_release);
                    initial = latest_;
                    initialRevision = revision_;
                    connected_.store(true, std::memory_order_release);
                }
                // Initial snapshot and its revision are captured atomically
                // against every subsequent enqueue. Never unlock the receive
                // stream for this subscriber until its complete baseline
                // is sent on the same ordered socket.
                bool ok = false;
                if (subscription.requested)
                    ok = writeSnapshot(
                        client, initial, initialRevision, subscription.pairs);
                else
                    ok = writeAllOrDisconnect(
                        client,
                        "{\"schema\":1,\"kind\":\"resync_required\",\"reason\":\"new_subscriber\"}\n");
                if (!ok)
                {
                    connected_.store(false, std::memory_order_release);
                    ::close(client);
                    client = -1;
                }
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
                if (frame.size() > 16 * 1024 * 1024 || !writeAllOrDisconnect(client, frame))
                {
                    std::fprintf(
                        stderr, "market-delta: frame too large or send failed; resync required\n");
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
