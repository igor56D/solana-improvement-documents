---
simd: 'XXXX'
title: Serial Execution Cost Tracking System
authors:
  - Igor Durovic (Anza)
  - Max Resnick (Anza)
category: Standard
type: Core
status: Review
created: 2025-07-15
feature: (fill in with feature key and github tracking issues once accepted)
---

## Summary

This proposal introduces a new parallelism-aware CU tracking algorithm and serial execution constraint.
The goal is to safely increase block CUs while mitigating problematic slow-replay edge cases that aren’t
covered by the existing block CU limit and per-account CU limit.

Correctly modeling compute resources allows for bounding replay time more accurately and safely increasing
the block CU limit faster. Specifically, the proposed system allows a theoretical maximum of 100M CUs per
block without requiring performance optimizations, just better accounting of resource usage.

note: this proposal will be followed up by another that removes the existing block CU limit.

## Motivation

In order to maximize transaction throughput while maintaining network resiliency and integrity, protocol
resource limits must be set at or near the maximum of what the lowest common denominator of client
implementations running on reference hardware can handle. Additionally, for these resource limits to
meaningfully represent real capacity, an accurate model of resource consumption is required. A protocol with
poor resource modeling must make zero-sum tradeoffs between safety and throughput.

The aggregate block-level CU limit doesn’t take parallel compute into consideration, and is therefore subject
to the same kind of zero-sum tradeoffs: capacity is left on the table and serial execution time isn't predictably
bounded. The per-account write limit attempts to prevent serial execution from dominating the block by
limiting hotspot account access. In practice, this isn't sufficient: the network produces 20M+ serial execution
CU blocks as a matter of course (and more rarely 25M+), despite the current 12M CU per-account limit.

Theoretically, the current constraint system even allows fully serial blocks, if carefully constructed to
circumvent the 12M per-account limit.

In short, maximally utilizing capacity on the Solana network without sacrificing safety requires a
parallelism-aware compute resource model. One such model is described in this proposal.

## New Terminology

- TX-DAG: a dependency graph where each node is a transaction and each edge represents execution dependencies between transactions that contend for the same account lock. The direction of the edge is determined by the relative position of each transaction in the block order.
- Track: ordered list of transactions belonging to a subset of transactions in a given block. Analogous to a serial execution schedule on a single thread.
- Vote Track: track dedicated to only simple vote transactions
- Deterministic Transaction Assignment Algorithm (DTAA): streaming algorithm that adds each incoming transaction to the TX-DAG and assigns it to a track based on previous assignments and the dependencies encoded by the TX-DAG.
- Critical Path: longest CU-weighted path in a TX-DAG
- Makespan: the highest CU track produced by DTAA to an entire block
- Serial Execution Limit: cap on makespan

## Detailed Design

High-level: the protocol cost tracker maintains CU counts for each execution track. As the cost tracker receives transactions to process in block order (either during block production or verification), it deterministically assigns each to an execution track and updates the track's CU amount to account for the CUs of the transaction, all of its parents in the `TX-DAG`, and all previous transactions assigned to the same track. This is equivalent to virtual scheduling, where each task's start time depends on completion of tasks that must complete before it.

### Protocol Changes and Additions

- Number of Standard Tracks: `N = 4`
- Number of Vote Tracks: `M = 1`
- Serial Execution CU Limit: `L = 25M`
- Deterministic Transaction Assignment Algorithm:
  - `DTAA(tx_stream, N, M) -> track_cus[]`
  - `track_cus[]` contains the serial execution CUs for each track
  - `tx_stream` is a real-time transaction stream representing input during block production or verification.
    - `DTAA` processes transactions from this stream as they arrive: assigning each to an appropriate track and adding it to the `TX-DAG`.
  - Assignment to a track sets that track's total CUs to `end(tx, track)`:
    - `end(tx, track) = start(tx, track) + tx.CUs`
    - `start(tx, track) =  max(track.CUs, max_path(tx))`
    - `max_path(tx) = max(end(p, p.track) for p in TX-DAG.parents(tx))`
  - Note: `tx.CUs` is the real CUs consumed during execution, not the CU limit requested by the fee payer.
  - Simple vote transactions are assigned to the `M` vote tracks, others to the `N` standard tracks.
- A block is valid iff:
  - `max(track.CUs for track in DTAA(tx_stream, N, M)) ≤ L`
  - Vote tracks are ignored

### Requirements

- Consensus: the serial execution limit doesn't apply to simple vote transactions. This ensures consensus performance isn’t negatively impacted.
- Determinism: for a given block, all validators must assign transactions to tracks identically so they agree on the makespan calculation. Otherwise, the cluster can fork due to block validity equivocation.
- Real-time: the assignment algorithm must work in a real-time, streaming context so CU-tracking can occur as shreds are being received by a validator or while a leader is building a block.
- A simple greedy scheduling algorithm fulfills these requirements: pseudocode provided below.
  - optimal assignment is NP-hard so sub-optimality is unavoidable.

Note: the actual execution does not need to abide by the schedule determined by the assignment. As long as the validator implementation is as good or better than the virtual model of the SVM, the actual scheduling, # of threads, etc. is irrelevant.

### Deterministic Transaction Assignment Algorithm

```
n                          // number of standard tracks
m                          // number of vote tracks
track_len[0‥n−1] = 0       // current length of each track in CUs
vote_track_len[0..m-1] = 0 // current length of each vote track in CUs
C = Map<Tx, CUs>           // longest path for each transaction
get_predecessors           // returns parents for a given tx
SERIAL_LIMIT               // limit on transaction assignment makespan

APPLY_TX_COST(t):
    // max_path(tx)
    r ← max(C[u] for u in get_predecessors(tx))

    chosen ← −1
    tracks ← track_len if is_simple_vote(t) else vote_track_len

    // Select track using the “tight-gap” heuristic:
    if min(tracks[i]) ≤ r:
      // use longest track with length ≤ r if it exists (minimize idle gap)
      candidate_tracks ← [tracks[i] for i where tracks[i] ≤ r]
      chosen ← i where tracks[i] = max(candidate_tracks[i]):
    else:
      // otherwise, fall back to classic Earliest-Finish rule
      chosen ← i where tracks[i] = min(tracks[i])

    start ← max(tracks[chosen], r)
    C[t] ← start + t.CUs
    if not is_simple_vote(t) and C[t] > SERIAL_LIMIT:
        INVALID_BLOCK

    tracks[chosen] ← C[t]
```

- `APPLY_TX_COST(tx)` must be called with transactions in the same order they appear in the block. This applies to both replayers and the leader.
- vote tracks aren't subject to the serial execution limit but we need to track them because they can still contend for the same accounts as
  transactions in the standard tracks, and be parents in the `TX-DAG` for those transactions.

### Implementation

- Block Verification (Replay): because real CUs rather than requested CUs are used for determining if constraints are satisfied, cost tracking must
  occur post-execution (i.e. `APPLY_TX_COST` must be called after `tx` is executed). Block execution during replay doesn't guarantee that transactions
  in a block will complete execution in the same order they appear in the block, so cost tracking must account for this somehow. For example, the cost
  tracker can handle re-ordering internally or a synchronization mechanism in the bank can enforce order.
- Block Production: similar post-execution requirements apply here as well; the main difference being that the position of a transaction in the block,
  in addition to the real CUs it consumes, isn't determined until post-execution when the transaction is processed by the PoH recorder. Caveat: this
  implies failure to satisfy the serial execution constraint may occur **after** a transaction has already been executed, which would waste compute
  resources.

### DTAA Optimality
For a block `B`, `max(critical_path(B), total_cus(B) / N)` represents a lower bound on optimal makespan `OPT`. If `lb(OPT)` is this lower bound and
`makespan` is the makespan calculated by `DTAA`, then `r = lb(OPT) / makespan` can be considered an lower bound estimate of the true optimality of `DTAA`.
The closer to 1 `r` is the more optimal DTAA is estimated to be. Empirical analysis shows that when applying `DTAA` retroactively to all mainnet blocks
with total CUs greater that 40M in a sample thousand slot range, the median `r` is `0.8`. More detailed data can be provided at request.

## Alternatives Considered

- critical path constraint: a limit on the critical path of the `TX-DAG`. This does restrict long serial chains of transactions but fails as a general
  serial execution constraint because it cannot account for the degree of parallel compute (e.g. number of threads).

## Impact

This proposal will likely require updates to transaction scheduling during block production, which includes MEV infrastructure like JITO. Cost tracking
will no longer be independent of order so shuffling transactions around while optimizing for profitability may impact block validity.

## Security Considerations

All validator client implementations must use the same DTAA in order to prevent equivocation on block validity. Otherwise, a partition of the network may
occur.
