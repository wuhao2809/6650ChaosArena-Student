output "base_url"     { value = "http://${aws_lb.main.dns_name}" }
output "ecr_repo_url" { value = aws_ecr_repository.main.repository_url }
output "albums_table" { value = aws_dynamodb_table.albums.name }
