#!/usr/bin/env bash
set -euo pipefail

REGION="us-west-2"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
APP="good-student"
IMAGE="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${APP}:latest"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

log() { echo "==> $*"; }

log "Ensuring default VPC exists in ${REGION}..."
VPC_ID=$(aws ec2 describe-vpcs \
  --filters "Name=isDefault,Values=true" \
  --query "Vpcs[0].VpcId" --output text --region "$REGION" 2>/dev/null || echo "None")
if [[ "$VPC_ID" == "None" || -z "$VPC_ID" ]]; then
  log "No default VPC found — creating one..."
  aws ec2 create-default-vpc --region "$REGION" > /dev/null
  log "Default VPC created."
else
  log "Default VPC exists: ${VPC_ID}"
fi

log "Tidying Go modules..."
cd "$SCRIPT_DIR"
go mod tidy

log "Running terraform..."
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

log "Done. Waiting ~2 min for ECS to deploy the new task before submitting."
terraform -chdir="$SCRIPT_DIR/terraform" output
