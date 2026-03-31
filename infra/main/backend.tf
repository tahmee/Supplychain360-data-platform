terraform {
  backend "s3" {
    bucket       = "supch360-tfstate-file"
    key          = "supplychain360/dev/terraform.tfstate"
    region       = "eu-central-1"
    use_lockfile = true
    encrypt      = true
  }
}