# Open Market Delta: initial PoC contract (XRPLF rippled 3.4.1)

> Status: **shadow-mode implementation in progress**. A Linux-only, disabled-by-default Unix socket stream and probe are implemented. There is no synchronized snapshot barrier, GitHub CI result, deployment controller, or bot integration yet. **Never use these events for live trading decisions**.

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

Event schema (current shadow prototype, with revision as a decimal string):

```json
{
  "schema": 1,
  "kind": "market_delta",
  "parentHash": "validated-parent-hash",
  "openLedger": 107866062,
  "revision": "123",
  "queueUs": "1400",
  "changes": [
    {
      "index": "...",
      "action": "modified",
      "before": {},
      "after": {}
    }
  ]
}
```

- `revision` is process-monotonic for queued notifications. `ledger_reset` marks each parent/epoch transition; `resync_required` marks a new subscriber, overrun, or encoding failure. At present notifications group every change within one published `OpenLedger::modify()`; they do **not** claim a unique `txHash`.
- Emit **only committed open-view changes**. Do not emit discarded Batch inner mutations, failed speculative applications, dry runs, or consensus replay as live market commits.
- Include `Offer`, `AMM`, and the account/trust-line changes needed to reconstruct AMM reserves. A change to AMM trading fees can invalidate a market even without an Offer update.
- The producer must copy/serialize data before the underlying view can be mutated or released. No holding view locks during socket I/O.
- A subscriber must explicitly synchronize a full starting market state and an epoch/revision watermark. A raw delta feed alone cannot initialize a correct order book.
- A gap, reorder, stale epoch, unsupported ledger object, or reconnect makes that part of the market **unknown**. The bot must fall back to RPC and reject stale candidates.

## Current implementation (shadow-only)

**Scope: four C++ files**, plus this documentation and a Python probe.

- `src/xrpld/app/ledger/detail/OpenLedger.cpp` queues a before/after open-view pair **only when `modify()` successfully publishes it**. Ledger `accept()` queues a reset. The Batch inner state does not leak until the enclosing published view is committed.
- `include/xrpl/ledger/detail/RawStateTable.h` exposes the union of locally modified ledger keys. `include/xrpl/ledger/OpenView.h` compares these keys in two immutable snapshots, so the worker does not sweep the entire account tree.
- `src/xrpld/app/ledger/detail/MarketDeltaTap.h` is a header-only optional Unix socket publisher. At most 32 snapshot events are queued; overflowing forces a `resync_required` event. Socket writing, ledger entry JSON serialization, and diff construction occur on a worker thread. Missing consumer means **no diff work**. The socket is owner-only (0600). It does not modify transaction application, consensus, ledger serialization, or signatures.
- Only changed `Offer`, `AMM`, `AccountRoot`, and `RippleState` objects are emitted. Market-affecting data not represented by these four entry types must be handled before trading use.
- The endpoint is **disabled by default**. This first stream does not provide a snapshot barrier or guarantee an initial complete book. A connection begins with `resync_required`; no trading client should treat its later deltas as fully initialized.
- Linux support only in this prototype. Build and integration correctness need verification in CI and on a **non-production** node first.

### Local shadow test

Build a test image from this branch and run it with the environment variable and a restrictive, shared socket directory:

```yaml
services:
  xrpld:
    environment:
      XRPLD_MARKET_DELTA_SOCKET: /run/market-delta/stream.sock
    volumes:
      - ./data/market-delta:/run/market-delta
```

The parent directory must already exist and be writable by xrpld. Do not change the production node image while experimenting. The socket is local only; no TCP port is opened.

After starting the **test** node, observe the stream from the host:

```bash
python3 tools/market_delta_probe.py /opt/xrpl-arb/server/data/market-delta/stream.sock --seconds 60
```

The observer checks JSON frames, sequence gaps, and epoch transitions. It does **not** establish snapshot consistency or prove XRP profitability. To compare against current-ledger RPC, build an independent shadow consumer with a synchronized snapshot/revision barrier first.

### Deployment and rollback

The planned `./ops/xrpld-forkctl {status|update|rollback}` controller is **not yet implemented**. No GHCR image has been produced by this prototype. Until CI, feed consistency, and rollback are verified, retain the official `xrplf/xrpld:3.4.1` as the LIVE node and preserve the NuDB volume. Never directly change LIVE to this prototype.

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
