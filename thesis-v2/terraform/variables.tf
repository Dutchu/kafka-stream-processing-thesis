variable "project_id" {
  description = "The GCP Project ID (same project as the original thesis-v1 stack)"
  type        = string
  default     = "example-project"
}

variable "region" {
  description = "The GCP region (same region as the original thesis-v1 stack)"
  type        = string
  default     = "europe-central2"
}

variable "zone" {
  description = "The GCP zone (same zone as the original thesis-v1 stack)"
  type        = string
  default     = "europe-central2-a"
}

variable "existing_vpc_name" {
  description = "Name of the pre-existing VPC network (thesis-v1 terraform/main.tf), read via a data source only — never (re-)created here."
  type        = string
  default     = "kafka-thesis-vpc"
}

variable "serverless_subnet_cidr" {
  description = "CIDR range for the new kafka-serverless-subnet (Direct VPC egress for Cloud Run functions gen2)."
  type        = string
  default     = "10.20.0.0/23"
}
