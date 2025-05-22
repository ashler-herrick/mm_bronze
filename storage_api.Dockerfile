FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends gcc libpq-dev git \
 && rm -rf /var/lib/apt/lists/*

# Copy your lockfiles and source in one go
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --locked

# Copy the entire mm_bronze package
COPY mm_bronze/ ./mm_bronze/

# Install it
RUN uv pip install .

ENV PYTHONPATH=/app
ENV PATH="/app/.venv/bin:$PATH"


CMD ["uv", "run", "python", "/app/mm_bronze/storage/api/app.py"]
