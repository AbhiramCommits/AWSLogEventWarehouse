# Terraform override file: points the AWS provider at LocalStack.
#
# Terraform merges this provider block into the `aws` provider declared in
# main.tf (override-file semantics: blocks with the same type + labels are
# merged, with the override file winning). Copy nothing else — applying the
# stack against real AWS simply means removing this file (or pointing
# AWS_ENDPOINT_URL at real AWS and dropping `endpoints`).

provider "aws" {
  endpoints {
    s3         = "http://localhost:4566"
    kinesis    = "http://localhost:4566"
    cloudwatch = "http://localhost:4566"
    logs       = "http://localhost:4566"
    sns        = "http://localhost:4566"
    iam        = "http://localhost:4566"
    sts        = "http://localhost:4566"
    # Glue + Firehose control planes are LocalStack "pro" features; the moto
    # sidecar (docker-compose service `moto`) serves those two locally.
    glue     = "http://localhost:4567"
    firehose = "http://localhost:4567"
  }
}
