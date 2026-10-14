#!/usr/bin/env bash
# Bootstrap the local AWS environment end-to-end:
#   1. create the virtualenv and install dependencies
#   2. start LocalStack + moto via docker compose and wait for health
#   3. terraform init + apply against LocalStack
#   4. provision the LocalStack "pro" resources (Glue DB, Firehose) with boto3
#   5. print the created resource names and the Kinesis streams
set -euo pipefail

export PATH="/Applications/Docker.app/Contents/Resources/bin:${HOME}/.local/bin:${PATH}"
cd "$(dirname "$0")/.."

if [ ! -x "${PWD}/.venv/bin/python" ]; then
  echo "==> Creating virtualenv + installing dependencies"
  make install
fi

echo "==> Starting LocalStack + moto"
docker compose up -d
.venv/bin/python scripts/wait_for_localstack.py

echo "==> terraform init + apply"
cd infra/terraform
terraform init -input=false
terraform apply -auto-approve -input=false
cd -

echo "==> Provisioning LocalStack pro resources (Glue DB, Firehose) via boto3"
.venv/bin/python scripts/bootstrap_local_aws.py

echo "==> terraform outputs"
cd infra/terraform && terraform output && cd -
