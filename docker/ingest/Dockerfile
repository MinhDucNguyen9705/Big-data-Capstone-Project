# cryptopipe-ingest: WebSocket producers and REST pollers (`crypto ingest ...`). Owner: A (Platform).
# Build from the repo root:  make build-images   (or: docker build -f docker/ingest/Dockerfile -t cryptopipe-ingest .)
# Run:                       docker run --rm cryptopipe-ingest:latest ingest binance --dry-run
# Size note: pyproject lists pyspark as a core dependency (~300 MB). A slimmer image needs pyspark moved to an
# extra; that is a pyproject change for its owners, not done here.
ARG PYTHON_IMAGE=python:3.11-slim-bookworm

FROM ${PYTHON_IMAGE} AS wheel
WORKDIR /src
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
RUN pip wheel --no-cache-dir --no-deps -w /wheels .

FROM ${PYTHON_IMAGE}
LABEL org.opencontainers.image.title="cryptopipe-ingest" \
      org.opencontainers.image.description="cryptopipe producers and pollers (crypto ingest ...)"
RUN useradd --create-home --uid 10001 app
WORKDIR /app

COPY --from=wheel /wheels /tmp/wheels
RUN pip install --no-cache-dir /tmp/wheels/*.whl && rm -rf /tmp/wheels /root/.cache

COPY conf ./conf
ENV CRYPTO_SYMBOLS_FILE=/app/conf/symbols.yaml \
    PYTHONUNBUFFERED=1
USER app
EXPOSE 8000
ENTRYPOINT ["crypto"]
CMD ["--help"]
