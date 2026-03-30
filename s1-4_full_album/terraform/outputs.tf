output "alb_dns" {
  description = "ALB DNS name — use this as the base URL when submitting to ChaosArena"
  value       = "http://${aws_lb.main.dns_name}"
}

output "s3_bucket" {
  value = aws_s3_bucket.photos.bucket
}

output "albums_table" {
  value = aws_dynamodb_table.albums.name
}

output "photos_table" {
  value = aws_dynamodb_table.photos.name
}
