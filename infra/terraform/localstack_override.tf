# Terraform override file: points the AWS provider at the local stack.
#
# Terraform merges this provider block into the `aws` provider declared in
# main.tf (override-file semantics: blocks with the same type + labels are
# merged, with the override file winning). Applying the stack against real AWS
# simply means removing this file (or setting `use_localstack = false`).
#
# Most services run on LocalStack (http://localhost:4566). LocalStack's
# community edition does not implement the Glue + Firehose control planes and
# ships an unusably slow kinesis-mock backend, so those three are served by the
# moto sidecar (http://localhost:4567) using identical AWS APIs.

provider "aws" {
  endpoints {
    s3         = "http://localhost:4566"
    cloudwatch = "http://localhost:4566"
    logs       = "http://localhost:4566"
    sns        = "http://localhost:4566"
    iam        = "http://localhost:4566"
    sts        = "http://localhost:4566"
    kinesis    = "http://localhost:4566"
    # LocalStack community lacks the Glue + Firehose control planes (pro
    # features), so the moto sidecar (docker-compose service `moto`) serves
    # those two locally through identical AWS APIs.
    glue     = "http://localhost:4567"
    firehose = "http://localhost:4567"
  }
}
