variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-west-2"
}

variable "app_name" {
  description = "Application name prefix for all resources"
  type        = string
  default     = "full-album"
}

variable "image_tag" {
  description = "Docker image tag to deploy"
  type        = string
  default     = "latest"
}
