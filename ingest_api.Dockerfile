FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      gcc \
      libpq-dev \
      git \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# copy lockfiles from repo root
COPY pyproject.toml uv.lock ./
RUN uv sync --locked

# copy package
COPY mm_bronze/ ./mm_bronze/

RUN uv pip install .

EXPOSE 8000

ENV PATH="/app/.venv/bin:$PATH"

CMD ["uvicorn", "mm_bronze.ingestion.api.app:app", "--host", "0.0.0.0", "--port", "8000"]
