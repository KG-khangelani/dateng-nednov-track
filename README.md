# Nedbank DE Challenge — Submission Scaffold

This repository is structured for scoring from the repository root, per `docs/submission_guide.md`.

## Repository Layout

```
your-submission/
├── Dockerfile
├── requirements.txt
├── pipeline/
├── config/
├── docs/                    # Reference docs from the challenge pack
├── infrastructure/          # Local tooling (for example: test harness)
└── README.md
```

### Stage-specific files
- Stage 2+: `config/dq_rules.yaml` is required.
- Stage 3 only: add `adr/stage3_adr.md` and streaming assets as required by the Stage 3 addendum.

## Quick Start

```bash
docker build -t my-submission:test .
bash infrastructure/run_tests.sh --stage 1 --data-dir /tmp/test-data --image my-submission:test
```

## References
- `docs/submission_guide.md`
- `docs/docker_interface_contract.md`
- `docs/output_schema_spec.md`
- `docs/validation_queries.sql`
