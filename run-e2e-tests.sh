#!/bin/bash
set -e

echo "🚀 Starting LocalStack for E2E tests..."

# Start LocalStack in the background
docker run -d \
  --name deltaglider-localstack \
  -p 4566:4566 \
  -e SERVICES=s3 \
  -e DEBUG=0 \
  -e DATA_DIR=/tmp/localstack/data \
  localstack/localstack:latest

echo "⏳ Waiting for LocalStack to be ready..."

# Wait for LocalStack to be healthy
max_attempts=30
attempt=0
while [ $attempt -lt $max_attempts ]; do
  if curl -s -f http://localhost:4566/_localstack/health > /dev/null 2>&1; then
    echo "✅ LocalStack is ready!"
    break
  fi
  echo "Waiting... (attempt $((attempt + 1))/$max_attempts)"
  sleep 2
  attempt=$((attempt + 1))
done

if [ $attempt -eq $max_attempts ]; then
  echo "❌ LocalStack failed to start within expected time"
  docker logs deltaglider-localstack
  docker rm -f deltaglider-localstack
  exit 1
fi

# Set environment variables and run tests
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
export AWS_ENDPOINT_URL=http://localhost:4566

echo "🧪 Running E2E tests..."
uv run pytest tests/e2e -v --tb=short

# Cleanup
echo "🧹 Cleaning up LocalStack..."
docker rm -f deltaglider-localstack

echo "✅ E2E tests completed!"