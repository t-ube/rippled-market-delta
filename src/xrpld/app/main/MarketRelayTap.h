#pragma once

// Relay-first local submission path for the Market Delta bot.
//
// Why: `submit` applies the transaction to this node's open ledger before it
// is relayed, and that application is serialized behind every inbound network
// transaction (150-300 ms under load, exactly when arbitrage happens). The
// validators do not need our local application result; they need the bytes.
//
// What: a second Unix socket (`<XRPLD_MARKET_DELTA_SOCKET>.relay`) accepts
// NDJSON `{"command":"relay","tx_blob":"<hex>","id":"<opaque>"}`. For each
// line the worker checks the signature and local rules (`checkValidity`, the
// same gate `submit` uses) and, if valid, hands the raw bytes to the overlay
// relay immediately. It does NOT apply the transaction locally: the bot still
// sends the normal `submit` afterwards, which applies it here and yields the
// engine result, and HashRouter::shouldRelay suppresses the second relay.
//
// Disabled unless XRPLD_MARKET_DELTA_SOCKET is set (same switch as the tap).
// Linux only. Never touches the ledger application thread.
#include <xrpld/app/ledger/LedgerMaster.h>
#include <xrpld/core/TimeKeeper.h>
#include <xrpld/overlay/Overlay.h>

#include <xrpl/basics/Slice.h>
#include <xrpl/basics/StringUtilities.h>
#include <xrpl/basics/base_uint.h>
#include <xrpl/core/HashRouter.h>
#include <xrpl/core/ServiceRegistry.h>
#include <xrpl/json/json_reader.h>
#include <xrpl/json/json_value.h>
#include <xrpl/json/to_string.h>
#include <xrpl/protocol/STTx.h>
#include <xrpl/protocol/Serializer.h>
#include <xrpl/protocol/TxFlags.h>
#include <xrpl/tx/apply.h>

#include <xrpl.pb.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
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

class MarketRelayTap final
{
public:
    // Start once, after the overlay exists (ApplicationImp::start). Safe to
    // call when the feature is off: it simply does nothing.
    static void
    start(ServiceRegistry& registry) noexcept
    {
#if defined(__linux__)
        static std::unique_ptr<MarketRelayTap> tap;
        if (tap)
            return;
        auto const* path = std::getenv("XRPLD_MARKET_DELTA_SOCKET");
        if (!path || !*path)
            return;
        try
        {
            tap.reset(new MarketRelayTap(registry, std::string(path) + ".relay"));
        }
        catch (...)
        {
            std::fprintf(stderr, "market-relay: optional relay socket unavailable\n");
        }
#endif
    }

    ~MarketRelayTap()
    {
        worker_.request_stop();
    }

private:
    ServiceRegistry& registry_;
    std::string const path_;
    std::jthread worker_;

    MarketRelayTap(ServiceRegistry& registry, std::string path)
        : registry_(registry), path_(std::move(path)), worker_([this](std::stop_token stop) {
            try
            {
                run(stop);
            }
            catch (...)
            {
                std::fprintf(stderr, "market-relay: worker stopped unexpectedly\n");
            }
        })
    {
    }

#if defined(__linux__)
    static bool
    writeAll(int fd, std::string const& line) noexcept
    {
        std::size_t sent = 0;
        auto const deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(100);
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
                if (::poll(&pfd, 1, 10) < 0 && errno != EINTR)
                    return false;
                continue;
            }
            return false;
        }
        return true;
    }

    // One request line -> one reply line. The whole path is signature
    // verification plus an overlay broadcast: no ledger lock, no job queue.
    std::string
    handle(std::string const& line)
    {
        auto const started = std::chrono::steady_clock::now();
        json::Value reply(json::ValueType::Object);
        json::Value request;
        json::Reader reader;
        auto const fail = [&](std::string const& error) {
            reply["ok"] = false;
            reply["error"] = error;
            return json::to_string(reply) + "\n";
        };
        if (!reader.parse(line, request) || !request.isObject())
            return fail("bad_json");
        if (request["id"].isString())
            reply["id"] = request["id"];
        if (!request["command"].isString() || request["command"].asString() != "relay" ||
            !request["tx_blob"].isString())
            return fail("bad_request");

        auto const blob = strUnHex(request["tx_blob"].asString());
        if (!blob || blob->empty())
            return fail("bad_hex");

        std::shared_ptr<STTx const> tx;
        try
        {
            SerialIter sit(makeSlice(*blob));
            tx = std::make_shared<STTx const>(sit);
        }
        catch (std::exception const& e)
        {
            return fail(std::string("bad_tx:") + e.what());
        }
        // Inner Batch transactions are never valid on the wire; this is the
        // same boundary check NetworkOPs::submitTransaction applies.
        if (tx->isFlag(tfInnerBatchTxn))
            return fail("inner_batch");

        auto const txid = tx->getTransactionID();
        reply["hash"] = to_string(txid);
        try
        {
            auto const [validity, reason] = checkValidity(
                registry_.getHashRouter(), *tx, registry_.getLedgerMaster().getValidatedRules());
            if (validity != Validity::Valid)
                return fail("invalid:" + reason);
        }
        catch (std::exception const& e)
        {
            return fail(std::string("check:") + e.what());
        }

        bool relayed = false;
        if (auto const toSkip = registry_.getHashRouter().shouldRelay(txid))
        {
            protocol::TMTransaction msg;
            Serializer s;
            tx->add(s);
            msg.set_rawtransaction(s.data(), s.size());
            msg.set_status(protocol::tsCURRENT);
            msg.set_receivetimestamp(registry_.getTimeKeeper().now().time_since_epoch().count());
            msg.set_deferred(false);
            // relayLocal: XRPLD_RELAY_LOCAL_MAX_PEERS caps the fan-out
            registry_.getOverlay().relayLocal(txid, msg, *toSkip);
            relayed = true;
        }
        reply["ok"] = true;
        reply["relayed"] = relayed;  // false: HashRouter saw it already (duplicate submit)
        reply["us"] = static_cast<unsigned>(std::chrono::duration_cast<std::chrono::microseconds>(
                                                std::chrono::steady_clock::now() - started)
                                                .count());
        return json::to_string(reply) + "\n";
    }

    void
    run(std::stop_token stop)
    {
        sockaddr_un addr{};
        if (path_.size() >= sizeof(addr.sun_path))
        {
            std::fprintf(stderr, "market-relay: socket path too long\n");
            return;
        }
        int const server = ::socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        if (server < 0)
            return;
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
            std::fprintf(stderr, "market-relay: unable to bind relay socket\n");
            ::close(server);
            return;
        }
        std::fprintf(stderr, "market-relay: listening on %s\n", path_.c_str());

        int client = -1;
        std::string buffer;
        char block[65536];
        while (!stop.stop_requested())
        {
            if (client < 0)
            {
                pollfd pfd{server, POLLIN, 0};
                if (::poll(&pfd, 1, 50) <= 0)
                    continue;
                client = ::accept4(server, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
                buffer.clear();
                continue;
            }
            pollfd pfd{client, POLLIN, 0};
            auto const ready = ::poll(&pfd, 1, 50);
            if (ready < 0 && errno == EINTR)
                continue;
            if (ready < 0 || (pfd.revents & (POLLERR | POLLHUP | POLLNVAL)))
            {
                ::close(client);
                client = -1;
                continue;
            }
            if (ready == 0)
                continue;
            auto const n = ::recv(client, block, sizeof(block), MSG_DONTWAIT);
            if (n == 0 || (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR))
            {
                ::close(client);
                client = -1;
                continue;
            }
            if (n < 0)
                continue;
            buffer.append(block, static_cast<std::size_t>(n));
            if (buffer.size() > 4 * 1024 * 1024)
            {
                ::close(client);
                client = -1;
                buffer.clear();
                continue;
            }
            std::size_t eol;
            while ((eol = buffer.find('\n')) != std::string::npos)
            {
                auto const line = buffer.substr(0, eol);
                buffer.erase(0, eol + 1);
                if (line.empty())
                    continue;
                std::string reply;
                try
                {
                    reply = handle(line);
                }
                catch (...)
                {
                    reply = "{\"ok\":false,\"error\":\"exception\"}\n";
                }
                if (!writeAll(client, reply))
                {
                    ::close(client);
                    client = -1;
                    buffer.clear();
                    break;
                }
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
