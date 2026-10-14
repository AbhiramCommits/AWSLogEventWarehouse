# Contributing

Thanks for your interest in AWSLogEventWarehouse.

## Development setup

```bash
make install     # create .venv and install the package + dev extras
make up          # start LocalStack + moto
make tf-apply    # provision the local AWS resources
make test        # ruff + pytest (with coverage)
```

The full loop for a change:

```bash
make lint
make test
make pipeline    # end-to-end run (resets local state first)
```

## Project layout

| Path | Purpose |
| --- | --- |
| `src/logwarehouse/` | settings + boto3 helpers |
| `src/producers/` | synthetic generators, Kinesis publisher, S3 delivery worker |
| `src/glue/` | Glue-compatible PySpark transform + Glue Catalog registration |
| `src/warehouse/` | DuckDB dimensional load + SQL tuning benchmark |
| `src/monitoring/` | CloudWatch metrics, alarms, incident simulator |
| `src/dashboard/` | Streamlit + Plotly dashboard |
| `sql/` | DuckDB / Redshift / Athena DDL + analytical queries |
| `infra/terraform/` | all infrastructure as code |
| `scripts/` | bootstrap, reset, end-to-end pipeline, screenshots |

## Conventions

- Python 3.11, type hints on public functions, docstrings on every public
  function.
- `ruff` must be clean (`make lint`); line length 100.
- Tests live in `tests/`; mark tests that need the running LocalStack stack
  with `@pytest.mark.integration`.
- Never commit secrets; use `.env.example` for configuration keys.
- If you add an artifact that should be reviewed (a report), write it to
  `artifacts/` as `.json` or `.md` so it is committed.

## Pull requests

1. Branch from `main`.
2. Keep PRs focused; include tests and update docs.
3. Ensure `make lint` and `make test` are green.
