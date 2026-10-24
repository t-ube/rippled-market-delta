# Open Market Delta: initial PoC contract (XRPLF rippled 3.4.1)

> Status: **design / shadow-mode only**. No daemon hook, stream endpoint, CI build, or deployment controller has been implemented by this document. Do not use these events to make live trading decisions until the verification gates below pass.

## Baseline

- Upstream source: tag `3.4.1`, commit `00e640751460c137636fdb6dae6b4280de355d81`.
- Production baseline is the official `xrplf/xrpld:3.4.1` Docker image. Preserve NuDB data, existing `xrpld.cfg`, and the admin WS endpoint `127.0.0.1:6006`.
- Do not fork consensus rules, ledger serialization, signature validation, Batch semantics, or transaction ordering.
- The trading bot uses `transactions_proposed` and refetches affected markets via `book_offers` and `amm_info`. Measured pair refetch latency in the supplied bot report was p50 56 ms / p90 89.4 ms. These are workload observations, not a guaranteed saving.

## Source inspection (3.4.1)

- `src/xrpld/app/ledger/detail/OpenLedger.cpp`: `OpenLedger::modify()` prepares a copy of the open view and publishes it under `currentMutex_` when changed; `OpenLedger::accept()` switches to a newly assembled open view at ledger transitions.
- `src/libxrpl/tx/apply.cpp`: `applyTransaction()` applies a Batch's inner transactions to `wholeBatchView` and commits their changes to the open view only if the Batch mode allows it. Observing inner attempts before this commit would produce **false market changes**.
- `src/libxrpl/ledger/ApplyViewImpl.cpp`: `ApplyViewImpl::visit()` can enumerate changed ledger entries including before/after, but it is a lower-level building block; capture/propagation through the transaction application path still needs implementation.
- `src/libxrpl/ledger/ApplyStateTable.cpp`: open ledger transaction metadata is normally not built (`!to.open() || isDryRun`), so the proposed stream cannot be treated as a reliable source of applied state diffs.

## Proposed transport and semantics

Preferred initial transport: local Unix domain socket with a bounded, nonblocking producer queue. Never expose the feed publicly. Queue saturation must not block consensus/application; it must publish a gap/resync marker, not silently lose changes.

Event schema, provisional:

```json
{
  "schema": 1,
  "kind": "market_delta",
  "epoch": "validated-parent-hash-or-equivalent",
  "openLedger": 107866062,
  "revision": 123,
  "txHash": "...",
  "changes": [
    {
      "ledgerIndex": "...",
      "entryType": "Offer",
      "action": "modified",
      "before": {},
      "after": {}
    }
  ]
}
```

- `revision` is strictly monotonic within an `epoch`; a new open-ledger parent begins a new epoch. Replays/resets/failed publishes must cause a resync marker.
- Emit **only committed open-view changes**. Do not emit discarded Batch inner mutations, failed speculative applications, dry runs, or consensus replay as live market commits.
- Include `Offer`, `AMM`, and the account/trust-line changes needed to reconstruct AMM reserves. A change to AMM trading fees can invalidate a market even without an Offer update.
- The producer must copy/serialize data before the underlying view can be mutated or released. No holding view locks during socket I/O.
- A subscriber must explicitly synchronize a full starting market state and an epoch/revision watermark. A raw delta feed alone cannot initialize a correct order book.
- A gap, reorder, stale epoch, unsupported ledger object, or reconnect makes that part of the market **unknown**. The bot must fall back to RPC and reject stale candidates.

## Deliver in stages

1. **Instrumentation only**: count eligible committed modifications and ledger epoch changes with runtime flag OFF by default; benchmark per-tx capture cost.
2. **Shadow stream**: publish state changes locally, plus revisions, snapshot barrier and gap signaling. Bot consumes only for metrics and compares with current-ledger RPC, without controlling orders.
3. **Verified state ingestion**: reconstruct affected books and AMM reserves in the bot; assert agreement with RPC under controlled cases and shadow traffic.
4. **Trading opt-in**: use the feed only for verified markets and epochs; keep the old `book_offers`/`amm_info` path as fail-closed fallback.
5. **Packaged deploy**: pinned GHCR digest or tagged image, health checks, and atomic operational rollback preserving NuDB.

## Required cases before trading opt-in

- Normal Offer created/modified/deleted; partial fill; expiration.
- AMM swap, reserve balance/trust line changes, `AMMVote` fee change, AMM deletion.
- Batch `tfAllOrNothing` success and inner failure/rollback (no leaked speculative diff).
- Repeated submissions, multiple outers containing the same inners, transaction retry/queue.
- Ledger accept/replay, epoch transition, slow subscriber, bounded queue overflow, disconnect/reconnect.
- Snapshot + delta consistency and multi-ledger compare against `book_offers` and `amm_info` pinned to a consistent ledger wherever the RPC supports it.

## Success gates (targets, not measured achievements)

- Zero silent sequence gaps and zero unflagged market-state divergences in shadow verification.
- More than 80% fewer Batch-related refetch RPC calls **without** reducing order correctness (target).
- Feed delivery latency measured from commit to Bot receive; initial p50 target <=3 ms.
- No material regression in peer sync, close times, node health, or process memory.
- Compare lane-specific first `submit()` p50/p90, +0 ledger inclusion, realized XRP PnL and fee spend over comparable windows.

## Operational boundary

The desired CLI interface is `./ops/xrpld-forkctl {status|update|rollback}`. It must not silently restart a live node and must verify that the bot is paused, a known-good image is retained, volumes are unchanged, and the restored node reaches `full`. This CLI does **not** exist yet. The initial prototype must never automatically change the production node.
