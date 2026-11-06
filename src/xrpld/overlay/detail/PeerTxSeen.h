#pragma once

// Pre-apply transaction tap for the Market Delta bot.
//
// Why: this node exposes a transaction to its clients (proposed stream,
// Market Delta frames) and relays it only after the transaction has waited in
// the job queue, had its signature checked and been applied to the open
// ledger. Under load that pipeline is serialized behind every other inbound
// transaction, hundreds of milliseconds to seconds, while the validators have
// long had the same bytes. The bot does not need the local engine result to
// react; it needs to know that the transaction exists. It verifies the
// signature itself and treats the resulting market change as tentative until
// the normal Market Delta frame confirms it.
//
// What: in PeerImp::handleTransaction, right after HashRouter reports the
// first copy of a transaction, the decoded STTx is queued (one lock, no I/O)
// and a worker thread writes one NDJSON line per transaction to a Unix socket:
//   {"kind":"tx_seen","hash":"...","t":"<unix ms>","peer":<id>,"addr":"<ip>","tx":{...}}
// `addr` is the delivering peer's address so the bot can rank peers by how
// often they are the first to deliver a market-moving transaction and pin
// the best ones with [ips_fixed].
// The node's own processing is unchanged: the job is still queued, checked and
// applied exactly as upstream, and nothing here touches a ledger or a lock
// that the application path holds.
//
// Enabled only when XRPLD_TX_SEEN_SOCKET names the socket path (for example
// <XRPLD_MARKET_DELTA_SOCKET>.txseen). Unset: one null-pointer check per
// transaction. Linux only. A slow or absent reader never blocks the node: the
// queue is bounded and frames that cannot be written in time are dropped.
#include <xrpld/overlay/Peer.h>

#include <xrpl/beast/net/IPEndpoint.h>
#include <xrpl/json/json_value.h>
#include <xrpl/json/to_string.h>
#include <xrpl/protocol/STTx.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
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

#if defined(__linux__)
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>

#include <poll.h>
#include <unistd.h>

#include <cerrno>
#endif

namespace xrpl {

class PeerTxSeen final
{
public:
    static PeerTxSeen*
    instance() noexcept
    {
#if defined(__linux__)
        static std::unique_ptr<PeerTxSeen> tap = [] {
            auto const* path = std::getenv("XRPLD_TX_SEEN_SOCKET");
            if (!path || !*path)
                return std::unique_ptr<PeerTxSeen>{};
            try
            {
                return std::unique_ptr<PeerTxSeen>(new PeerTxSeen(path));
            }
            catch (...)
            {
                std::fprintf(stderr, "tx-seen: optional tap unavailable\n");
                return std::unique_ptr<PeerTxSeen>{};
            }
        }();
        return tap.get();
#else
        return nullptr;
#endif
    }

    ~PeerTxSeen()
    {
        worker_.request_stop();
        ready_.notify_all();
    }

    // Called on the peer's I/O thread for the first copy of a transaction.
    // Only a bounded queue push; the worker owns serialization and I/O.
    void
    seen(
        std::shared_ptr<STTx const> tx,
        Peer::id_t peer,
        beast::ip::Endpoint const& address) noexcept
    {
        try
        {
            auto const now = std::chrono::duration_cast<std::chrono::milliseconds>(
                                 std::chrono::system_clock::now().time_since_epoch())
                                 .count();
            auto addr = address.address().to_string();
            {
                std::lock_guard const lock(mutex_);
                if (queue_.size() >= kMaxQueue)
                {
                    ++dropped_;
                    return;
                }
                queue_.push_back(
                    Item{std::move(tx), peer, std::move(addr), static_cast<std::int64_t>(now)});
            }
            ready_.notify_one();
        }
        catch (...)
        {
        }
    }

private:
    struct Item
    {
        std::shared_ptr<STTx const> tx;
        Peer::id_t peer;
        std::string addr;
        std::int64_t atMs;
    };

    static constexpr std::size_t kMaxQueue = 8192;

    std::string const path_;
    std::mutex mutex_;
    std::condition_variable ready_;
    std::deque<Item> queue_;
    std::atomic<std::uint64_t> dropped_{0};
    std::uint64_t written_ = 0;
    std::uint64_t unwritable_ = 0;
    std::jthread worker_;

    explicit PeerTxSeen(std::string path)
        : path_(std::move(path)), worker_([this](std::stop_token stop) {
            try
            {
                run(stop);
            }
            catch (...)
            {
                std::fprintf(stderr, "tx-seen: worker stopped unexpectedly\n");
            }
        })
    {
    }

    static std::string
    frame(Item const& item)
    {
        json::Value line(json::ValueType::Object);
        line["kind"] = "tx_seen";
        line["hash"] = to_string(item.tx->getTransactionID());
        line["t"] = std::to_string(item.atMs);  // unix ms; json::Value has no 64-bit integer
        line["peer"] = static_cast<unsigned>(item.peer);
        line["addr"] = item.addr;
        line["tx"] = item.tx->getJson(JsonOptions::Values::None);
        return json::to_string(line) + "\n";
    }

#if defined(__linux__)
    // Non-blocking write with a short deadline. A reader that cannot keep up
    // loses frames instead of holding the queue.
    static bool
    writeAll(int fd, std::string const& line) noexcept
    {
        std::size_t sent = 0;
        auto const deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(20);
        while (sent < line.size())
        {
            auto const n =
                ::send(fd, line.data() + sent, line.size() - sent, MSG_NOSIGNAL | MSG_DONTWAIT);
            if (n > 0)
            {
                sent += static_cast<std::size_t>(n);
                continue;
            }
            if (n < 0 && errno == EINTR)
                continue;
            if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
            {
                if (std::chrono::steady_clock::now() > deadline)
                    return false;
                pollfd pfd{fd, POLLOUT, 0};
                if (::poll(&pfd, 1, 5) < 0 && errno != EINTR)
                    return false;
                continue;
            }
            return false;
        }
        return true;
    }

    int
    listenOn()
    {
        sockaddr_un addr{};
        if (path_.size() >= sizeof(addr.sun_path))
        {
            std::fprintf(stderr, "tx-seen: socket path too long\n");
            return -1;
        }
        int const server = ::socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        if (server < 0)
            return -1;
        std::memcpy(addr.sun_path, path_.c_str(), path_.size() + 1);
        addr.sun_family = AF_UNIX;

        struct stat existing{};
        if (::lstat(path_.c_str(), &existing) == 0)
        {
            // Only ever replace our own stale socket.
            if (!S_ISSOCK(existing.st_mode) || existing.st_uid != ::geteuid() ||
                ::unlink(path_.c_str()) != 0)
            {
                ::close(server);
                return -1;
            }
        }
        else if (errno != ENOENT)
        {
            ::close(server);
            return -1;
        }
        if (::bind(server, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0 ||
            ::chmod(path_.c_str(), S_IRUSR | S_IWUSR) != 0 || ::listen(server, 1) != 0)
        {
            std::fprintf(stderr, "tx-seen: unable to bind socket\n");
            ::close(server);
            return -1;
        }
        std::fprintf(stderr, "tx-seen: listening on %s\n", path_.c_str());
        return server;
    }

    void
    run(std::stop_token stop)
    {
        int const server = listenOn();
        if (server < 0)
            return;

        int client = -1;
        std::deque<Item> batch;
        auto lastReport = std::chrono::steady_clock::now();
        char sink[4096];
        while (!stop.stop_requested())
        {
            if (client < 0)
            {
                // No reader: keep the queue empty so a late reader starts fresh.
                {
                    std::lock_guard const lock(mutex_);
                    queue_.clear();
                }
                pollfd pfd{server, POLLIN, 0};
                if (::poll(&pfd, 1, 50) <= 0)
                    continue;
                client = ::accept4(server, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
                if (client >= 0)
                    std::fprintf(stderr, "tx-seen: reader connected\n");
                continue;
            }

            {
                std::unique_lock lock(mutex_);
                ready_.wait_for(lock, std::chrono::milliseconds(50), [&] {
                    return stop.stop_requested() || !queue_.empty();
                });
                batch.swap(queue_);
            }

            // Drain anything the reader sent (it never should) and notice a
            // hangup without waiting for a failed write.
            pollfd pfd{client, POLLIN, 0};
            if (::poll(&pfd, 1, 0) > 0)
            {
                auto const n = ::recv(client, sink, sizeof(sink), MSG_DONTWAIT);
                if (n == 0 || (pfd.revents & (POLLERR | POLLHUP | POLLNVAL)))
                {
                    ::close(client);
                    client = -1;
                    batch.clear();
                    std::fprintf(stderr, "tx-seen: reader disconnected\n");
                    continue;
                }
            }

            for (auto const& item : batch)
            {
                std::string line;
                try
                {
                    line = frame(item);
                }
                catch (...)
                {
                    ++unwritable_;
                    continue;
                }
                if (!writeAll(client, line))
                {
                    ++unwritable_;
                    break;  // the rest of this batch is stale; drop it
                }
                ++written_;
            }
            batch.clear();

            auto const now = std::chrono::steady_clock::now();
            if (now - lastReport >= std::chrono::minutes(10))
            {
                lastReport = now;
                std::fprintf(
                    stderr,
                    "tx-seen: written %llu dropped(queue full) %llu unwritable %llu\n",
                    static_cast<unsigned long long>(written_),
                    static_cast<unsigned long long>(dropped_.load()),
                    static_cast<unsigned long long>(unwritable_));
            }
        }
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
