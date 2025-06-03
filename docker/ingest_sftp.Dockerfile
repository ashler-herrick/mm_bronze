# docker/ingest_sftp.Dockerfile
FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      gcc \
      libpq-dev \
      git \
      openssh-client \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy lockfiles and install dependencies
COPY pyproject.toml uv.lock ./
RUN uv sync --locked

# Copy the entire mm_bronze package
COPY mm_bronze/ ./mm_bronze/

# Install the package
RUN uv pip install .

# Create directories for uploads and SSH keys with proper permissions
RUN mkdir -p /uploads /app/keys /app/keys/users \
 && chmod 755 /uploads \
 && chmod 700 /app/keys \
 && chmod 755 /app/keys/users

# Create non-root user for security
RUN groupadd -r sftpserver && useradd -r -g sftpserver -d /app -s /bin/bash sftpserver \
 && chown -R sftpserver:sftpserver /app /uploads

# Set environment variables
ENV PYTHONPATH=/app
ENV PATH="/app/.venv/bin:$PATH"
ENV SFTP_UPLOAD_ROOT=/uploads
ENV SFTP_HOST_KEY_PATH=/app/keys/ssh_host_rsa_key
ENV SFTP_KEYS_DIR=/app/keys/users

# Switch to non-root user
USER sftpserver

# Expose SFTP port
EXPOSE 2222

# Run the SFTP server
CMD ["uv", "run", "python", "/app/mm_bronze/ingestion/sftp/app.py"]