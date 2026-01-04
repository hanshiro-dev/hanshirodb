# Build stage
FROM rust:1.75-slim as builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    protobuf-compiler \
    libprotobuf-dev \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /usr/src/hanshirodb

# Copy manifests
COPY Cargo.toml Cargo.lock ./
COPY hanshiro-core/Cargo.toml ./hanshiro-core/
COPY hanshiro-storage/Cargo.toml ./hanshiro-storage/
COPY hanshiro-index/Cargo.toml ./hanshiro-index/
COPY hanshiro-ingest/Cargo.toml ./hanshiro-ingest/
COPY hanshiro-api/Cargo.toml ./hanshiro-api/
COPY hanshiro-cli/Cargo.toml ./hanshiro-cli/

# Create dummy source files for dependency caching
RUN mkdir -p hanshiro-core/src hanshiro-storage/src hanshiro-index/src \
    hanshiro-ingest/src hanshiro-api/src hanshiro-cli/src && \
    echo "fn main() {}" > hanshiro-cli/src/main.rs && \
    touch hanshiro-core/src/lib.rs hanshiro-storage/src/lib.rs \
    hanshiro-index/src/lib.rs hanshiro-ingest/src/lib.rs hanshiro-api/src/lib.rs

# Build dependencies (cached layer)
RUN cargo build --release && \
    rm -rf hanshiro-*/src

# Copy actual source code
COPY . .

# Touch main.rs to ensure rebuild
RUN touch hanshiro-cli/src/main.rs

# Build release binary
RUN cargo build --release --all-features

# Runtime stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Create user for running HanshiroDB
RUN useradd -m -u 1000 -s /bin/bash hanshiro

# Create data directories
RUN mkdir -p /var/lib/hanshirodb /etc/hanshirodb && \
    chown -R hanshiro:hanshiro /var/lib/hanshirodb /etc/hanshirodb

# Copy binary from builder
COPY --from=builder /usr/src/hanshirodb/target/release/hanshiro-cli /usr/local/bin/hanshirodb
COPY --from=builder /usr/src/hanshirodb/target/release/hanshiro-api /usr/local/bin/hanshiro-api

# Copy default config
COPY config/hanshirodb.toml /etc/hanshirodb/

# Switch to non-root user
USER hanshiro

# Expose ports
EXPOSE 9090 9091 8080

# Volume for data
VOLUME ["/var/lib/hanshirodb"]

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD hanshirodb health || exit 1

# Default command
CMD ["hanshiro-api", "--config", "/etc/hanshirodb/hanshirodb.toml"]