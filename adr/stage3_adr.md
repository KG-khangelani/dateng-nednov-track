# Architecture Decision Record: Stage 3 Streaming Extension

**Author:** Candidate
**Date:** 2026-04-27
**Status:** Final

## Context

Stage 3 added a near-real-time requirement for mobile account balances and recent transactions. The interface is intentionally lightweight: JSONL micro-batch files are mounted under `/data/stream`, discovered by filename, and written into two Delta tables under `/data/output/stream_gold`. The batch medallion pipeline still has to run first so that stream events can be validated against the Gold account dimension.

The implemented pipeline is organized around small stage modules and shared helpers in `pipeline/common.py`. Batch execution still follows Bronze, Silver, and Gold, while `pipeline/stream_ingest.py` extends the same parsing and normalization patterns for Stage 3. The runtime constraints remain the same as Stage 1 and Stage 2: no network access, no interactive prompts, Delta output, 2 vCPU, and 2 GB memory.

## Decision 1: How The Existing Architecture Helped Or Hindered Stage 3

The shared config and Spark setup made Stage 3 easier because paths, Delta reads/writes, transaction parsing, date parsing, and currency normalization live outside the individual medallion modules. `pipeline/stream_ingest.py` can reuse those behaviors instead of having a separate JSON parser or a second Spark setup. Keeping `run_all.py` as an orchestrator also helped: adding streaming only required one stage check after Gold provisioning, so the batch contract stayed intact.

The batch-first design still creates some friction. Stream state is mutable, while Bronze/Silver/Gold batch outputs are overwrite-oriented. To keep the implementation simple under the challenge constraints, the streaming tables use read-union-rank-overwrite state updates rather than a larger framework. That is acceptable for twelve small micro-batches, but it is not a production streaming architecture. Roughly most of the Stage 1 and Stage 2 code survives intact; Stage 3 extends it with one new module and shared helper reuse rather than rewriting the batch path.

## Decision 2: What I Would Change In Stage 1 In Hindsight

If I were starting again, I would define all table schemas and output contracts in a dedicated schema module from the first stage. The final implementation centralizes some parsing in `pipeline/common.py`, but Gold schemas are still assembled in `pipeline/provision.py` and stream schemas in `pipeline/stream_ingest.py`. When Stage 3 added `current_balances` and `recent_transactions`, this split meant there were multiple places to verify field order and nullability.

I would also design the state update interface earlier. Stage 1 and Stage 2 naturally encourage full-table overwrites because the challenge is batch-oriented. Stage 3 needs account-level upsert behavior and last-50 retention. If that had been visible from the start, I would have introduced a small `StateStore` abstraction around Delta tables so batch dimensions, DQ quarantine tables, and stream gold tables all used the same write semantics vocabulary.

## Decision 3: How I Would Design It With Full Stage Visibility

With all three stages known from day one, I would keep the same medallion shape but build two source adapters: one for bounded batch files and one for ordered micro-batch directories. Both would emit a common transaction DataFrame with parsed timestamp, normalized amount, normalized currency, and quality flags. Silver would own validation and conformance, while Gold would own dimensional joins and stateful serving tables.

For stream state, I would still choose Delta because it is already part of the base image and matches the scoring reader. I would make the upsert path explicit instead of relying on overwrite-after-merge patterns. The entry point would remain `pipeline/run_all.py`, but it would call a shared stage runner that understands `batch` and `stream` modes and records state transitions. That would make the Stage 3 addition feel like another configured source rather than a separate late-stage extension.
