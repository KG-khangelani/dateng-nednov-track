# Nedbank DE Challenge — Canonical Operator Guide

This `README.md` is the canonical runbook for operating this repository locally and in scoring-like conditions.

> `README_FIRST.md` is retained as challenge-pack reference material only. For build/run/test instructions, always follow this file.

## 1) Purpose and architecture

This repository implements the DE-track pipeline entry point expected by the scorer: `python pipeline/run_all.py`. The execution model is the **medallion pattern**:

- **Bronze**: raw ingestion from source files into Delta tables (`ingest.py`).
- **Silver**: cleaning/standardisation/DQ flagging (`transform.py`).
- **Gold**: scored business outputs (`provision.py`).

The top-level flow is deterministic and non-interactive:

1. `run_ingestion()`
2. `run_transformation()`
3. `run_provisioning()`

No prompts or stdin reads are allowed in the scoring container (no TTY attached).

## 2) Data sources and expected outputs

### Expected input mount points

The scoring contract mounts challenge data at fixed container paths:

- `/data/input/accounts.csv`
- `/data/input/customers.csv`
- `/data/input/transactions.jsonl`
- `/data/config/pipeline_config.yaml`
- `/data/config/dq_rules.yaml` (**required from Stage 2 onward**)
- `/data/stream/` (Stage 3 streaming micro-batches)

### Expected output paths

Your pipeline must write to the exact output roots below:

- `/data/output/bronze/`
- `/data/output/silver/`
- `/data/output/gold/`
- `/data/output/dq_report.json` (Stage 2+)
- `/data/output/stream_gold/` (Stage 3)

Gold tables expected by validation queries:

- `/data/output/gold/fact_transactions`
- `/data/output/gold/dim_accounts`
- `/data/output/gold/dim_customers`

## 3) Exact local build/run commands (scoring-like constraints)

### 3.1 Build

```bash
docker build -t my-submission:test .
```

### 3.2 Prepare local test mount

Create a local directory shaped exactly like the scorer mount:

```bash
mkdir -p /tmp/test-data/input /tmp/test-data/config /tmp/test-data/output
cp /path/to/accounts.csv /tmp/test-data/input/
cp /path/to/customers.csv /tmp/test-data/input/
cp /path/to/transactions.jsonl /tmp/test-data/input/
cp config/pipeline_config.yaml /tmp/test-data/config/
# Stage 2+ only:
cp config/dq_rules.yaml /tmp/test-data/config/
```

### 3.3 Run with scorer-equivalent resource/security flags

```bash
docker run --rm \
  --network=none \
  --memory=2g --memory-swap=2g \
  --cpus=2 \
  --pids-limit=512 \
  --read-only \
  --tmpfs /tmp:rw,size=512m \
  --cap-drop=ALL \
  --security-opt no-new-privileges \
  -e PYTHONDONTWRITEBYTECODE=1 \
  -v /tmp/test-data/input:/data/input:ro \
  -v /tmp/test-data/config:/data/config:ro \
  -v /tmp/test-data/output:/data/output:rw \
  my-submission:test \
  python pipeline/run_all.py
```

For Stage 3, add:

```bash
-v /tmp/test-stream:/data/stream:ro
```

## 4) Pipeline execution flow and non-interactive behavior

The required execution command is:

```bash
python pipeline/run_all.py
```

`pipeline/run_all.py` orchestrates ingest → transform → provision in sequence and is the official entry point used by automated scoring.

Operational requirements:

- Do **not** add `input()` or any blocking prompt logic.
- Do **not** rely on stdin.
- Exit code must be `0` on success.

## 5) Validation instructions

### 5.1 Run the local test harness

```bash
bash infrastructure/run_tests.sh --stage 1 --data-dir /tmp/test-data --image my-submission:test
```

Stage-specific harness examples:

```bash
bash infrastructure/run_tests.sh --stage 2 --data-dir /tmp/test-data --image my-submission:test
bash infrastructure/run_tests.sh --stage 3 --data-dir /tmp/test-data --stream-dir /tmp/test-stream --image my-submission:test
```

### 5.2 Run SQL validation checks

Use the provided SQL checks in `docs/validation_queries.sql` against your Gold output.

Quick DuckDB flow:

```sql
INSTALL delta;
LOAD delta;
SET VARIABLE gold_path = '/data/output/gold';
.read docs/validation_queries.sql
```

Or from shell:

```bash
duckdb < docs/validation_queries.sql
```

## 6) Stage-specific operator notes

### Stage 1

- Batch-only flow is sufficient.
- `dq_rules.yaml` not required.

### Stage 2

- `config/dq_rules.yaml` is required and must drive DQ handling behavior.
- `dq_report.json` is expected at `/data/output/dq_report.json`.

### Stage 3

- Implement streaming ingestion for files delivered via `/data/stream/`.
- Write streaming outputs under `/data/output/stream_gold/`.
- Include ADR at `adr/stage3_adr.md`.
- Batch pipeline compatibility from Stage 2 must remain intact.

## 7) Reference documents

- `docs/submission_guide.md`
- `docs/README_DOCKER.md`
- `docs/output_schema_spec.md`
- `docs/validation_queries.sql`
- `docs/stage2_spec_addendum.md`
- `docs/stage3_spec_addendum.md`
