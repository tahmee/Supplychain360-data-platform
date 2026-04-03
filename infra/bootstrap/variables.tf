variable "statefile_bucket" {
  description = "S3 bucket for Terraform state"
  type        = string
  default     = "supch360-tfstate-file"
}

variable "project_name" {
  description = "Project name used for tagging"
  type        = string
  default     = "supplychain360"
}

variable "environment" {
  description = "Deployment environment"
  type        = string
  default     = "dev"
}