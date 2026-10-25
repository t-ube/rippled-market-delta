#pragma once

// Shadow-only local stream for committed open-ledger state changes.
// Disabled unless XRPLD_MARKET_DELTA_SOCKET is set. No socket I/O or JSON
// serialization is done on the ledger application thread.
#include <xrpl/json/json_reader.h>
#include <xrpl/json/json_value.h>
#include <xrpl/json/to_string.h>
#include <xrpl/ledger/BookDirs.h>
#include <xrpl/ledger/Ledger.h>
#include <xrpl/ledger/OpenView.h>
#include <xrpl/protocol/Book.h>
#include <xrpl/protocol/Indexes.h>
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
#include <set>
#include <stop_token>
#include <string>
#include <thread>
#include <utility>
#include <vector>

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
    initialize(Snapshot initial) noexcept
    {
        try
        {
            std::lock_guard const lock(mutex_);
            latest_ = std::move(initial);
        }
        catch (...)
        {
        }
    }

    void
    reset(
        Snapshot before,
        Snapshot after,
        std::shared_ptr<Ledger const> oldValidated,
        std::shared_ptr<Ledger const> newValidated) noexcept
    {
        enqueue(
            Kind::Reset,
            std::move(before),
            std::move(after),
            std::move(oldValidated),
            std::move(newValidated));
    }

private:
    enum class Kind { Delta, Reset };
    struct Event
    {
        Kind kind;
        std::uint64_t revision;
        Snapshot before;
        Snapshot after;
        std::shared_ptr<Ledger const> oldValidated;
        std::shared_ptr<Ledger const> newValidated;
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
    enqueue(
        Kind kind,
        Snapshot before,
        Snapshot after,
        std::shared_ptr<Ledger const> oldValidated = {},
        std::shared_ptr<Ledger const> newValidated = {}) noexcept
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
                {kind,
                 revision,
                 std::move(before),
                 std::move(after),
                 std::move(oldValidated),
                 std::move(newValidated),
                 now,
                 revision});
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
    writeAllOrDisconnect(
        int fd,
        std::string const& line,
        std::chrono::milliseconds budget = std::chrono::milliseconds(100)) noexcept
    {
        auto const deadline = std::chrono::steady_clock::now() + budget;
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

    // The initial snapshot establishes the exact set of objects held by
    // the subscriber. Only changes to those keys, plus offers/AMMs for
    // subscribed pairs, may be sent in subsequent frames. Newly observed
    // market makers gain their unchanged funding entries in the SAME frame.
    static std::string
    encodeScoped(
        Event const& event,
        std::set<std::string> const& pairs,
        std::set<uint256>& watched)
    {
        if (!event.before || !event.after || pairs.empty())
            return {};

        bool const advance = event.kind == Kind::Reset;
        std::set<uint256> keys;
        if (advance)
        {
            if (!event.oldValidated || !event.newValidated ||
                event.newValidated->seq() != event.oldValidated->seq() + 1 ||
                event.after->seq() != event.before->seq() + 1 ||
                event.after->header().parentHash != event.newValidated->header().hash ||
                event.before->header().parentHash != event.oldValidated->header().hash ||
                event.newValidated->header().parentHash != event.oldValidated->header().hash)
                return {};

            constexpr int kMaxValidatedChanges = 50000;
            SHAMap::Delta validated;
            if (!event.newValidated->stateMap().compare(
                    event.oldValidated->stateMap(), validated, kMaxValidatedChanges))
                return {};
            for (auto const& [key, change] : validated)
                keys.insert(key);
        }
        event.after->visitChangesSince(
            *event.before,
            [&](uint256 const& key, SLE::const_pointer, SLE::const_pointer) { keys.insert(key); });
        if (keys.size() > 50000)
            return {};

        json::Value out(json::ValueType::Object);
        out["schema"] = 1;
        out["kind"] = advance ? "ledger_advance" : "market_delta";
        out["revision"] = std::to_string(event.revision);
        if (event.coalesced != 0)
        {
            out["revisionFrom"] = std::to_string(event.firstRevision);
            out["coalesced"] = std::to_string(event.coalesced);
        }
        if (advance)
        {
            out["fromOpenLedger"] = static_cast<unsigned>(event.before->seq());
            out["fromParentHash"] = to_string(event.before->header().parentHash);
        }
        out["openLedger"] = static_cast<unsigned>(event.after->seq());
        out["parentHash"] = to_string(event.after->header().parentHash);
        out["queueUs"] = std::to_string(
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - event.queued)
                .count());

        // First pass discovers newly relevant makers before deciding which
        // account/trustline state to include. Never omit unchanged balances
        // just because only the maker's new Offer appeared in the delta.
        std::set<uint256> newlyWatched;
        auto trackFunding = [&](SLE::const_pointer const& sle) {
            if (!sle)
                return;
            if (sle->getType() != ltOFFER && sle->getType() != ltAMM)
                return;
            auto const data = sle->getJson();
            if (!data["Account"].isString())
                return;
            auto const maker = parseBase58<AccountID>(data["Account"].asString());
            if (!maker)
                return;
            auto trackKey = [&](uint256 const& key) {
                if (watched.insert(key).second)
                    newlyWatched.insert(key);
            };
            trackKey(keylet::account(*maker).key);
            auto addLine = [&](json::Value const& value) {
                Issue issue;
                if (parseAsset(assetKey(value), issue) && !isXRP(issue) &&
                    issue.account != *maker)
                    trackKey(keylet::trustLine(*maker, issue).key);
            };
            if (sle->getType() == ltOFFER)
                addLine(data["TakerGets"]);
            else
            {
                addLine(data["Asset"]);
                addLine(data["Asset2"]);
            }
        };

        // Preserve a stable old watch set for the whole frame. Newly watched
        // funding keys are inserted as synthetic creates after market changes.
        std::set<uint256> eligible;
        for (auto const& key : keys)
        {
            if (watched.contains(key))
            {
                eligible.insert(key);
                continue;
            }
            auto const before = event.before->read(Keylet{ltANY, key});
            auto const after = event.after->read(Keylet{ltANY, key});
            auto const matches = [&](SLE::const_pointer const& sle) {
                if (!sle || (sle->getType() != ltOFFER && sle->getType() != ltAMM))
                    return false;
                auto const data = sle->getJson();
                auto const pair = sle->getType() == ltOFFER
                    ? pairKey(assetKey(data["TakerPays"]), assetKey(data["TakerGets"]))
                    : pairKey(assetKey(data["Asset"]), assetKey(data["Asset2"]));
                return pairs.contains(pair);
            };
            if (matches(before) || matches(after))
            {
                watched.insert(key);
                eligible.insert(key);
                trackFunding(after);
            }
        }
        // Watchers from the baseline also need to discover funding for
        // a new seller if the Offer's index was already in the baseline.
        for (auto const& key : eligible)
        {
            auto const after = event.after->read(Keylet{ltANY, key});
            if (after && (after->getType() == ltOFFER || after->getType() == ltAMM))
                trackFunding(after);
        }

        json::Value changes(json::ValueType::Array);
        std::size_t emitted = 0;
        auto addChange = [&](uint256 const& key, bool synthetic) {
            auto const after = event.after->read(Keylet{ltANY, key});
            auto const before =
                synthetic ? SLE::const_pointer{} : event.before->read(Keylet{ltANY, key});
            if ((!marketEntry(before) && !marketEntry(after)) ||
                (before && after && *before == *after))
                return;
            json::Value change(json::ValueType::Object);
            change["index"] = to_string(key);
            change["action"] = !before ? "created" : !after ? "deleted" : "modified";
            change["before"] = before ? before->getJson() : json::Value{};
            change["after"] = after ? after->getJson() : json::Value{};
            changes.append(std::move(change));
            ++emitted;
        };
        for (auto const& key : eligible)
            addChange(key, false);
        for (auto const& key : newlyWatched)
            if (!eligible.contains(key))
                addChange(key, true);
        out["changes"] = std::move(changes);
        auto frame = json::to_string(out) + "\n";
        if (advance)
            std::fprintf(
                stderr,
                "market-delta: ledger_advance open=%u candidates=%zu changes=%zu bytes=%zu watched=%zu\n",
                static_cast<unsigned>(event.after->seq()),
                keys.size(),
                emitted,
                frame.size(),
                watched.size());
        return frame;
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
            !request["command"].isString() || request["command"].asString() != "snapshot" ||
            !request["pairs"].isArray() || request["pairs"].size() == 0 ||
            request["pairs"].size() > 4096)
            return sub;
        for (auto const& p : request["pairs"])
        {
            if (!p.isString() || p.asString().size() > 192 ||
                p.asString().find('|') == std::string::npos)
            {
                std::fprintf(
                    stderr, "market-delta: invalid subscription pair (length or separator)\n");
                sub.pairs.clear();
                return sub;
            }
            sub.pairs.insert(p.asString());
        }
        return sub;
    }

    static bool
    parseAsset(std::string const& name, Issue& out)
    {
        if (name == "XRP")
        {
            out = xrpIssue();
            return true;
        }
        auto const dot = name.find('.');
        if (dot == std::string::npos || name.find('.', dot + 1) != std::string::npos)
            return false;
        Currency currency;
        if (!toCurrency(currency, name.substr(0, dot)) || isXRP(currency) ||
            currency == badCurrency())
            return false;
        auto const account = parseBase58<AccountID>(name.substr(dot + 1));
        if (!account || *account == noAccount() || *account == xrpAccount())
            return false;
        out = Issue{currency, *account};
        return true;
    }

    // Read just the subscribed books and keyed funding entries; never walk
    // view->sles (the validated SHAMap may have tens of millions of nodes).
    // BookDirs visits both directions in quality order and includes inherited
    // validated offers, with no book-depth truncation.
    static bool
    writeSnapshot(
        int client,
        Snapshot const& view,
        std::uint64_t revision,
        std::set<std::string> const& pairs,
        std::set<uint256>* watched = nullptr)
    {
        if (!view || pairs.empty())
        {
            std::fprintf(
                stderr, "market-delta: snapshot unavailable (view or subscription empty)\n");
            return false;
        }
        auto const started = std::chrono::steady_clock::now();
        json::Value begin(json::ValueType::Object);
        begin["schema"] = 1;
        begin["kind"] = "snapshot_begin";
        begin["revision"] = std::to_string(revision);
        begin["openLedger"] = static_cast<unsigned>(view->seq());
        begin["parentHash"] = to_string(view->header().parentHash);
        begin["pairCount"] = static_cast<unsigned>(pairs.size());
        constexpr auto kSnapshotSendBudget = std::chrono::milliseconds(2000);
        if (!writeAllOrDisconnect(client, json::to_string(begin) + "\n", kSnapshotSendBudget))
        {
            std::fprintf(stderr, "market-delta: snapshot_begin send failed\n");
            return false;
        }

        json::Value entries(json::ValueType::Array);
        std::set<uint256> emitted;
        std::set<uint256> missingFundingKeys;
        std::set<AccountID> accounts;
        std::set<std::pair<AccountID, Issue>> trustLines;
        std::size_t count = 0;
        std::size_t bufferedBytes = 0;
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
            return writeAllOrDisconnect(client, json::to_string(part) + "\n", kSnapshotSendBudget);
        };
        auto add = [&](SLE::const_pointer const& sle) {
            if (!sle || !emitted.insert(sle->key()).second)
                return true;
            json::Value item(json::ValueType::Object);
            item["index"] = to_string(sle->key());
            item["after"] = sle->getJson();
            auto const bytes = json::to_string(item).size();
            if (bufferedBytes + bytes > 128 * 1024 && !flush())
                return false;
            bufferedBytes += bytes;
            entries.append(std::move(item));
            ++count;
            return true;
        };
        auto funding = [&](SLE::const_pointer const& sle, Issue const& sold) {
            if (!sle)
                return false;
            auto const data = sle->getJson();
            if (!data["Account"].isString())
                return false;
            auto const owner = parseBase58<AccountID>(data["Account"].asString());
            if (!owner)
                return false;
            accounts.insert(*owner);
            if (!isXRP(sold) && *owner != sold.account)
                trustLines.emplace(*owner, sold);
            return true;
        };

        // One keyed AMM read and two quality-ordered book walks per pair.
        // All issuer AccountRoots are included for TransferRate and flags.
        for (auto const& pair : pairs)
        {
            auto const sep = pair.find('|');
            if (sep == std::string::npos)
                return false;
            Issue left;
            Issue right;
            if (!parseAsset(pair.substr(0, sep), left) ||
                !parseAsset(pair.substr(sep + 1), right) || left == right)
            {
                std::fprintf(stderr, "market-delta: invalid snapshot asset pair\n");
                return false;
            }
            if (!isXRP(left))
                accounts.insert(left.account);
            if (!isXRP(right))
                accounts.insert(right.account);

            for (auto const& book :
                 {Book{left, right, std::nullopt}, Book{right, left, std::nullopt}})
            {
                for (auto const& offer : BookDirs(*view, book))
                {
                    if (!offer)
                        continue;
                    Issue const& sold = book.out.get<Issue>();
                    if (!funding(offer, sold) || !add(offer))
                        return false;
                }
            }
            if (auto const amm = view->read(keylet::amm(left, right)))
            {
                auto const data = amm->getJson();
                if (!data["Account"].isString())
                    return false;
                auto const owner = parseBase58<AccountID>(data["Account"].asString());
                if (!owner)
                    return false;
                accounts.insert(*owner);
                if (!isXRP(left) && *owner != left.account)
                    trustLines.emplace(*owner, left);
                if (!isXRP(right) && *owner != right.account)
                    trustLines.emplace(*owner, right);
                if (!add(amm))
                    return false;
            }
        }

        // A Book/AMM owner is queried only for the issue it sells/holds.
        // This avoids the O(owners × all assets) cross product.
        for (auto const& account : accounts)
        {
            missingFundingKeys.insert(keylet::account(account).key);
            if (!add(view->read(keylet::account(account))))
                return false;
        }
        for (auto const& [owner, issue] : trustLines)
        {
            missingFundingKeys.insert(keylet::trustLine(owner, issue).key);
            if (!add(view->read(keylet::trustLine(owner, issue))))
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
        auto const ok =
            writeAllOrDisconnect(client, json::to_string(end) + "\n", kSnapshotSendBudget);
        auto const elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                                 std::chrono::steady_clock::now() - started)
                                 .count();
        std::fprintf(
            stderr,
            "market-delta: snapshot_end open=%u pairs=%zu entries=%zu ms=%lld ok=%d\n",
            static_cast<unsigned>(view->seq()),
            pairs.size(),
            count,
            static_cast<long long>(elapsed),
            static_cast<int>(ok));
        if (ok && watched)
        {
            watched->swap(emitted);
            watched->insert(missingFundingKeys.begin(), missingFundingKeys.end());
        }
        return ok;
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
        std::set<std::string> subscribedPairs;
        std::set<uint256> watched;
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
                subscribedPairs = subscription.pairs;
                watched.clear();
                std::fprintf(
                    stderr,
                    "market-delta: subscriber request=%d pairs=%zu\n",
                    static_cast<int>(subscription.requested),
                    subscribedPairs.size());
                Snapshot initial;
                std::uint64_t initialRevision = 0;
                {
                    std::deque<Event> retired;
                    {
                        std::lock_guard const lock(mutex_);
                        retired.swap(pending_);
                        disconnect_.store(false, std::memory_order_release);
                        initial = latest_;
                        initialRevision = revision_;
                        connected_.store(true, std::memory_order_release);
                    }
                    // Drop potentially large snapshots outside the ledger mutex.
                }
                // Initial snapshot and its revision are captured atomically
                // against every subsequent enqueue. Never unlock the receive
                // stream for this subscriber until its complete baseline
                // is sent on the same ordered socket.
                bool ok = false;
                if (subscription.requested)
                    ok = writeSnapshot(
                        client, initial, initialRevision, subscription.pairs, &watched);
                else
                    ok = writeAllOrDisconnect(
                        client,
                        "{\"schema\":1,\"kind\":\"resync_required\",\"reason\":\"new_subscriber\"}"
                        "\n");
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
                    std::deque<Event> retired;
                    retired.swap(pending_);
                    lock.unlock();
                    retired.clear();  // Snapshot destruction must not block enqueue().
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
                // In steady state an epoch transition carries only changed
                // state keys. Snapshot is the recovery path, not the normal
                // ledger-reset path. A bounded compare failure revokes trust.
                if (event.kind == Kind::Reset && !subscribedPairs.empty())
                {
                    auto const advance = encodeScoped(event, subscribedPairs, watched);
                    if (advance.empty() || advance.size() > 16 * 1024 * 1024 ||
                        !writeAllOrDisconnect(client, advance, std::chrono::milliseconds(2000)))
                    {
                        (void)writeAllOrDisconnect(
                            client,
                            "{\"schema\":1,\"kind\":\"resync_required\","
                            "\"reason\":\"ledger_advance_unavailable\"}\n");
                        ::close(client);
                        client = -1;
                    }
                    continue;
                }
                auto const frame = subscribedPairs.empty()
                    ? encode(event)
                    : encodeScoped(event, subscribedPairs, watched);
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
