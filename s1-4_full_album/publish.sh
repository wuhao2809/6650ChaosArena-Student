#!/usr/bin/env bash
set -euo pipefail

REGION="us-west-2"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
APP="full-album"
IMAGE="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${APP}:latest"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

log() { echo "==> $*"; }

log "Tidying Go modules (generates go.sum)..."
cd "$SCRIPT_DIR"
go mod tidy

log "Running terraform (creates ECR, DynamoDB, S3, ECS, ALB)..."
terraform -chdir="$SCRIPT_DIR/terraform" init -upgrade
terraform -chdir="$SCRIPT_DIR/terraform" apply -auto-approve \
  -var="aws_region=${REGION}" \
  -var="image_tag=latest"

log "Logging in to ECR..."
aws ecr get-login-password --region "$REGION" \
  | docker login --username AWS --password-stdin "${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com"

log "Building image (linux/amd64)..."
docker build --platform linux/amd64 -t "$IMAGE" "$SCRIPT_DIR"

log "Pushing image..."
docker push "$IMAGE"

log "Forcing ECS redeployment..."
aws ecs update-service --cluster "$APP" --service "$APP" \
  --force-new-deployment --region "$REGION" --output text > /dev/null

log "Done."
terraform -chdir="$SCRIPT_DIR/terraform" output
