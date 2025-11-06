# Docker Support for DeltaGlider

This document describes how to build, run, and publish Docker images for DeltaGlider.

## Quick Start

### Pull and run the latest image

```bash
docker pull beshultd/deltaglider:latest
docker run --rm beshultd/deltaglider:latest --help
```

### Run with AWS credentials

```bash
docker run --rm \
  -e AWS_ACCESS_KEY_ID=your_key \
  -e AWS_SECRET_ACCESS_KEY=your_secret \
  -e AWS_DEFAULT_REGION=us-east-1 \
  beshultd/deltaglider:latest ls s3://your-bucket/
```

### Run with MinIO (local S3 alternative)

```bash
# Start MinIO
docker run -d \
  -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  --name minio \
  minio/minio server /data --console-address ":9001"

# Use DeltaGlider with MinIO
docker run --rm \
  -e AWS_ENDPOINT_URL=http://host.docker.internal:9000 \
  -e AWS_ACCESS_KEY_ID=minioadmin \
  -e AWS_SECRET_ACCESS_KEY=minioadmin \
  -e AWS_DEFAULT_REGION=us-east-1 \
  beshultd/deltaglider:latest ls
```

## Building Locally

### Build with current git version

```bash
VERSION=$(git describe --tags --always --abbrev=0 | sed 's/^v//')
docker build --build-arg VERSION=${VERSION} -t beshultd/deltaglider:${VERSION} .
```

### Build with custom version

```bash
docker build --build-arg VERSION=6.0.2 -t beshultd/deltaglider:6.0.2 .
```

### Multi-platform build

```bash
# Create a buildx builder (one-time setup)
docker buildx create --name deltaglider-builder --use

# Build for multiple platforms
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  --build-arg VERSION=6.0.2 \
  -t beshultd/deltaglider:6.0.2 \
  --push \
  .
```

## Testing the Image

### Basic functionality test

```bash
# Check version
docker run --rm beshultd/deltaglider:test --version

# Check help
docker run --rm beshultd/deltaglider:test --help

# List available commands
docker run --rm beshultd/deltaglider:test
```

### Integration test with MinIO

```bash
# 1. Start MinIO
docker run -d \
  -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  --name minio \
  minio/minio server /data --console-address ":9001"

# 2. Create a test file
echo "Hello DeltaGlider" > test.txt

# 3. Upload to S3/MinIO
docker run --rm \
  -v $(pwd):/data \
  -w /data \
  -e AWS_ENDPOINT_URL=http://host.docker.internal:9000 \
  -e AWS_ACCESS_KEY_ID=minioadmin \
  -e AWS_SECRET_ACCESS_KEY=minioadmin \
  -e AWS_DEFAULT_REGION=us-east-1 \
  beshultd/deltaglider:test cp test.txt s3://test-bucket/

# 4. List bucket contents
docker run --rm \
  -e AWS_ENDPOINT_URL=http://host.docker.internal:9000 \
  -e AWS_ACCESS_KEY_ID=minioadmin \
  -e AWS_SECRET_ACCESS_KEY=minioadmin \
  -e AWS_DEFAULT_REGION=us-east-1 \
  beshultd/deltaglider:test ls s3://test-bucket/

# 5. Get statistics
docker run --rm \
  -e AWS_ENDPOINT_URL=http://host.docker.internal:9000 \
  -e AWS_ACCESS_KEY_ID=minioadmin \
  -e AWS_SECRET_ACCESS_KEY=minioadmin \
  -e AWS_DEFAULT_REGION=us-east-1 \
  beshultd/deltaglider:test stats test-bucket

# 6. Cleanup
docker stop minio && docker rm minio
rm test.txt
```

## Publishing to Docker Hub

### Manual Publishing

```bash
# 1. Log in to Docker Hub
docker login

# 2. Build the image
VERSION=$(git describe --tags --always --abbrev=0 | sed 's/^v//')
docker build --build-arg VERSION=${VERSION} \
  -t beshultd/deltaglider:${VERSION} \
  -t beshultd/deltaglider:latest \
  .

# 3. Push to Docker Hub
docker push beshultd/deltaglider:${VERSION}
docker push beshultd/deltaglider:latest
```

### Multi-platform Publishing

```bash
# Create builder (one-time setup)
docker buildx create --name deltaglider-builder --use

# Build and push for multiple platforms
VERSION=$(git describe --tags --always --abbrev=0 | sed 's/^v//')
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  --build-arg VERSION=${VERSION} \
  -t beshultd/deltaglider:${VERSION} \
  -t beshultd/deltaglider:latest \
  --push \
  .
```

## GitHub Actions Automation

The repository includes a GitHub Action workflow (`.github/workflows/docker-publish.yml`) that automatically builds and publishes Docker images.

### Automatic Publishing Triggers

- **On main branch push**: Tags as `latest`
- **On develop branch push**: Tags as `develop`
- **On version tag push** (e.g., `v6.0.2`): Tags with semver patterns:
  - `6.0.2` (full version)
  - `6.0` (major.minor)
  - `6` (major)
- **On pull request**: Builds but doesn't push (testing only)

### Required GitHub Secrets

Set these secrets in your GitHub repository settings (`Settings > Secrets and variables > Actions`):

1. **DOCKERHUB_USERNAME**: Your Docker Hub username (e.g., `beshultd`)
2. **DOCKERHUB_TOKEN**: Docker Hub access token (create at https://hub.docker.com/settings/security)

### Manual Workflow Trigger

You can manually trigger the Docker build workflow from the GitHub Actions tab:

1. Go to **Actions** tab
2. Select **Build and Publish Docker Images**
3. Click **Run workflow**
4. Select branch and click **Run workflow**

## Docker Image Details

### Image Layers

The Dockerfile uses a multi-stage build:

1. **Builder stage**: Installs UV and Python dependencies
2. **Runtime stage**: Minimal Python 3.12-slim with only runtime dependencies

### Image Features

- **Size**: ~150MB (compressed)
- **Platforms**: linux/amd64, linux/arm64
- **User**: Runs as non-root user `deltaglider` (UID 1000)
- **Base**: Python 3.12-slim (Debian)
- **Dependencies**:
  - Python 3.12
  - xdelta3 (binary diff tool)
  - All Python dependencies from `pyproject.toml`

### Environment Variables

The image supports the following environment variables:

```bash
# Logging
DG_LOG_LEVEL=INFO              # DEBUG, INFO, WARNING, ERROR

# Performance & Compression
DG_MAX_RATIO=0.5               # Max delta/file ratio (0.0-1.0)

# Cache Configuration
DG_CACHE_BACKEND=filesystem    # filesystem or memory
DG_CACHE_MEMORY_SIZE_MB=100    # Memory cache size
DG_CACHE_ENCRYPTION_KEY=       # Optional encryption key

# AWS Configuration
AWS_ENDPOINT_URL=              # S3 endpoint (for MinIO/LocalStack)
AWS_ACCESS_KEY_ID=             # AWS access key
AWS_SECRET_ACCESS_KEY=         # AWS secret key
AWS_DEFAULT_REGION=us-east-1   # AWS region
```

### Health Check

The image includes a health check that runs every 30 seconds:

```bash
docker inspect --format='{{.State.Health.Status}}' <container-id>
```

## Troubleshooting

### Build Issues

#### "setuptools-scm was unable to detect version"

**Cause**: Git metadata not available during build.

**Solution**: Always use the `VERSION` build arg:

```bash
docker build --build-arg VERSION=6.0.2 -t beshultd/deltaglider:6.0.2 .
```

#### Cache issues

**Cause**: Docker build cache causing stale builds.

**Solution**: Use `--no-cache` flag:

```bash
docker build --no-cache --build-arg VERSION=6.0.2 -t beshultd/deltaglider:6.0.2 .
```

### Runtime Issues

#### "unauthorized: access token has insufficient scopes"

**Cause**: Not logged in to Docker Hub or invalid credentials.

**Solution**:

```bash
docker login
# Enter your Docker Hub credentials
```

#### "Cannot connect to MinIO/LocalStack"

**Cause**: Using `localhost` instead of `host.docker.internal` from inside container.

**Solution**: Use `host.docker.internal` for Mac/Windows or `172.17.0.1` for Linux:

```bash
# Mac/Windows
-e AWS_ENDPOINT_URL=http://host.docker.internal:9000

# Linux
-e AWS_ENDPOINT_URL=http://172.17.0.1:9000
```

## Docker Compose

For local development with MinIO:

```yaml
version: '3.8'

services:
  minio:
    image: minio/minio:latest
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    command: server /data --console-address ":9001"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
      interval: 10s
      timeout: 5s
      retries: 5

  deltaglider:
    image: beshultd/deltaglider:latest
    environment:
      AWS_ENDPOINT_URL: http://minio:9000
      AWS_ACCESS_KEY_ID: minioadmin
      AWS_SECRET_ACCESS_KEY: minioadmin
      AWS_DEFAULT_REGION: us-east-1
      DG_LOG_LEVEL: DEBUG
    depends_on:
      - minio
    volumes:
      - ./data:/data
    working_dir: /data
    command: ["--help"]
```

Run with:

```bash
docker-compose up -d
docker-compose run --rm deltaglider ls
```

## Best Practices

1. **Always specify version**: Use `--build-arg VERSION=x.y.z` when building
2. **Use multi-stage builds**: Keeps final image small
3. **Tag with semantic versions**: Follow semver (major.minor.patch)
4. **Test before pushing**: Run integration tests locally
5. **Use secrets**: Never hardcode credentials in images
6. **Multi-platform builds**: Support both amd64 and arm64
7. **Update README**: Keep Docker Hub description in sync with README.md

## Additional Resources

- [Docker Hub Repository](https://hub.docker.com/r/beshultd/deltaglider)
- [GitHub Repository](https://github.com/beshu-tech/deltaglider)
- [MinIO Documentation](https://min.io/docs/minio/container/index.html)
- [Docker Buildx Documentation](https://docs.docker.com/buildx/working-with-buildx/)
