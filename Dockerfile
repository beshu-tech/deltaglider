# Multi-stage build for deltaglider
ARG PYTHON_VERSION=3.12-slim
ARG UV_VERSION=0.5.13

# Builder stage - install UV and dependencies
FROM ghcr.io/astral-sh/uv:$UV_VERSION AS uv
FROM python:${PYTHON_VERSION} AS builder

# Copy UV from the UV image
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_SYSTEM_PYTHON=1

WORKDIR /build

# Copy dependency files first for better caching
COPY pyproject.toml ./
COPY README.md ./

# Install dependencies with UV caching
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --compile-bytecode .

# Copy source code
COPY src ./src

# Install the package (force reinstall to ensure it's properly installed)
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --compile-bytecode --no-deps --force-reinstall .

# Runtime stage - minimal image
FROM python:${PYTHON_VERSION}

# Skip man pages and docs to speed up builds
RUN mkdir -p /etc/dpkg/dpkg.cfg.d && \
    echo 'path-exclude /usr/share/doc/*' > /etc/dpkg/dpkg.cfg.d/01_nodoc && \
    echo 'path-exclude /usr/share/man/*' >> /etc/dpkg/dpkg.cfg.d/01_nodoc && \
    echo 'path-exclude /usr/share/groff/*' >> /etc/dpkg/dpkg.cfg.d/01_nodoc && \
    echo 'path-exclude /usr/share/info/*' >> /etc/dpkg/dpkg.cfg.d/01_nodoc && \
    echo 'path-exclude /usr/share/lintian/*' >> /etc/dpkg/dpkg.cfg.d/01_nodoc && \
    echo 'path-exclude /usr/share/linda/*' >> /etc/dpkg/dpkg.cfg.d/01_nodoc

# Install xdelta3 (now much faster without man pages)
RUN apt-get update && \
    apt-get install -y --no-install-recommends xdelta3 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -u 1000 -s /bin/bash deltaglider

# Copy installed packages from builder
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin/deltaglider /usr/local/bin/deltaglider

# Set up working directory
WORKDIR /app
RUN chown -R deltaglider:deltaglider /app

# Create cache directory with proper permissions
RUN mkdir -p /tmp/.deltaglider && \
    chown -R deltaglider:deltaglider /tmp/.deltaglider

USER deltaglider

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD deltaglider --help || exit 1

# Labels
LABEL org.opencontainers.image.title="DeltaGlider" \
      org.opencontainers.image.description="Delta-aware S3 file storage wrapper" \
      org.opencontainers.image.version="0.1.0" \
      org.opencontainers.image.authors="Beshu Limited" \
      org.opencontainers.image.source="https://github.com/beshu-tech/deltaglider"

ENTRYPOINT ["deltaglider"]
CMD ["--help"]