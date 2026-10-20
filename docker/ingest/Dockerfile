# Producers and pollers image (first draft from the bootstrap; owner A finalizes).
# Build from the repo root:  docker build -f docker/ingest/Dockerfile -t cryptopipe-ingest:dev .
# Run:                       docker run --rm cryptopipe-ingest:dev ingest binance --dry-run
FROM python:3.11-slim-bookworm

RUN useradd --create-home --uid 10001 app
WORKDIR /app

COPY pyproject.toml README.md LICENSE ./
COPY src ./src
# Note for A: this pulls pyspark (~300 MB) as a dependency; a slimmer image could move Spark deps to an extra.
RUN pip install --no-cache-dir . && rm -rf src build

COPY conf ./conf

ENV CRYPTO_SYMBOLS_FILE=/app/conf/symbols.yaml \
    PYTHONUNBUFFERED=1
USER app
EXPOSE 8000
ENTRYPOINT ["crypto"]
CMD ["--help"]
