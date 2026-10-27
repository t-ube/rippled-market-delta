#pragma once

// Which peers sit next to the validators?
//
// Why: the Market Delta bot relays its transactions to all peers and the
// validators only see them one or more hops later. Knowing which peers are the
// first to hand us each validator's proposals and validations tells us which
// peers are adjacent to (or are the sentinels of) those validators, so they
// can be pinned with [ips_fixed] and put first on the relay path.
//
// What: a passive counter. For every trusted proposal and validation that
// reaches this node for the first time (HashRouter said it was not a
// duplicate), remember the delivering peer. Every N ledgers print one line per
// peer to stderr, best first, and reset the window counts.
//
// Enabled only when XRPLD_PEER_FIRST_SEEN is set to the report interval in
// ledgers (e.g. 100). Unset: a single null-pointer check per message.
#include <xrpld/overlay/Peer.h>

#include <xrpl/beast/net/IPEndpoint.h>
#include <xrpl/protocol/PublicKey.h>
#include <xrpl/protocol/tokens.h>

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

namespace xrpl {

class PeerFirstSeen
{
public:
    static PeerFirstSeen*
    instance() noexcept
    {
        static std::unique_ptr<PeerFirstSeen> tap = [] {
            auto const* env = std::getenv("XRPLD_PEER_FIRST_SEEN");
            if (!env || !*env)
                return std::unique_ptr<PeerFirstSeen>{};
            auto const interval = std::strtol(env, nullptr, 10);
            if (interval <= 0)
                return std::unique_ptr<PeerFirstSeen>{};
            return std::unique_ptr<PeerFirstSeen>(
                new PeerFirstSeen(static_cast<std::uint32_t>(interval)));
        }();
        return tap.get();
    }

    void
    noteProposal(PublicKey const& validator, Peer::id_t peer, beast::ip::Endpoint const& address)
    {
        std::lock_guard lock(mutex_);
        auto& stat = statFor(peer, address);
        ++stat.proposals;
        ++proposals_;
        stat.validators.insert(shortKey(validator));
    }

    void
    noteValidation(
        PublicKey const& validator,
        std::uint32_t ledgerSeq,
        Peer::id_t peer,
        beast::ip::Endpoint const& address)
    {
        std::lock_guard lock(mutex_);
        auto& stat = statFor(peer, address);
        ++stat.validations;
        ++validations_;
        stat.validators.insert(shortKey(validator));
        if (ledgerSeq > lastSeq_)
            lastSeq_ = ledgerSeq;
        if (firstSeq_ == 0)
            firstSeq_ = ledgerSeq;
        if (lastSeq_ >= firstSeq_ + interval_)
            report();
    }

private:
    struct PeerStat
    {
        std::string address;
        std::uint64_t proposals = 0;
        std::uint64_t validations = 0;
        std::set<std::string> validators;
    };

    explicit PeerFirstSeen(std::uint32_t interval) : interval_(interval)
    {
    }

    static std::string
    shortKey(PublicKey const& key)
    {
        return toBase58(TokenType::NodePublic, key).substr(0, 10);
    }

    PeerStat&
    statFor(Peer::id_t peer, beast::ip::Endpoint const& address)
    {
        auto& stat = peers_[peer];
        if (stat.address.empty())
            stat.address = address.address().to_string();
        return stat;
    }

    // Called with mutex_ held. One line per peer, most validator-adjacent
    // first, then the window starts over.
    void
    report()
    {
        std::vector<std::pair<Peer::id_t, PeerStat const*>> rows;
        rows.reserve(peers_.size());
        for (auto const& [id, stat] : peers_)
            rows.emplace_back(id, &stat);
        std::sort(rows.begin(), rows.end(), [](auto const& a, auto const& b) {
            return a.second->validations + a.second->proposals >
                b.second->validations + b.second->proposals;
        });
        std::fprintf(
            stderr,
            "peer-first-seen: ledgers %u..%u trusted proposals %llu validations %llu "
            "delivering peers %zu\n",
            firstSeq_,
            lastSeq_,
            static_cast<unsigned long long>(proposals_),
            static_cast<unsigned long long>(validations_),
            peers_.size());
        std::size_t shown = 0;
        for (auto const& [id, stat] : rows)
        {
            if (shown++ >= 25)
                break;
            std::ostringstream keys;
            std::size_t n = 0;
            for (auto const& k : stat->validators)
            {
                if (n++)
                    keys << ',';
                if (n > 12)
                {
                    keys << "...";
                    break;
                }
                keys << k;
            }
            std::fprintf(
                stderr,
                "peer-first-seen:   peer %u %s  val_first %llu (%.0f%%)  prop_first %llu (%.0f%%)  "
                "validators %zu: %s\n",
                static_cast<unsigned>(id),
                stat->address.c_str(),
                static_cast<unsigned long long>(stat->validations),
                validations_ ? 100.0 * stat->validations / validations_ : 0.0,
                static_cast<unsigned long long>(stat->proposals),
                proposals_ ? 100.0 * stat->proposals / proposals_ : 0.0,
                stat->validators.size(),
                keys.str().c_str());
        }
        std::fflush(stderr);
        peers_.clear();
        proposals_ = 0;
        validations_ = 0;
        firstSeq_ = lastSeq_;
    }

    std::uint32_t const interval_;
    std::mutex mutex_;
    std::unordered_map<Peer::id_t, PeerStat> peers_;
    std::uint64_t proposals_ = 0;
    std::uint64_t validations_ = 0;
    std::uint32_t firstSeq_ = 0;
    std::uint32_t lastSeq_ = 0;
};

}  // namespace xrpl
