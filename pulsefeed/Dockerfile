# PulseFeed API server.
#
#   docker build -t pulsefeed .
#   docker run --env-file .env -p 8000:8000 pulsefeed
#
# Or, with the backing services from docker-compose.yml:
#
#   docker compose --profile app up -d
#
# The image contains no secrets and no model files; everything is injected
# through the environment (see .env.example) and, for a fitted scorer, a
# volume mount pointed at by PULSEFEED_SCORER_MODEL.

FROM python:3.11-slim

# A non-root user: the process needs no privileges and should have none.
RUN useradd --create-home --uid 1000 pulsefeed
WORKDIR /app

# Install dependencies first so source edits do not bust the layer cache.
COPY pyproject.toml README.md ./
COPY pulsefeed ./pulsefeed
COPY harness ./harness
RUN pip install --no-cache-dir '.[all,store]'

USER pulsefeed
EXPOSE 8000

# Preflight at container start is deliberate: a container that boots with a
# broken DSN should say so in its logs and exit, not serve amnesiac traffic.
# Set PULSEFEED_SKIP_PREFLIGHT=1 to bypass (e.g. while backends are still
# coming up under an orchestrator that will restart us anyway).
CMD ["sh", "-c", "\
  if [ \"$PULSEFEED_SKIP_PREFLIGHT\" != \"1\" ]; then pulsefeed-preflight || exit 1; fi; \
  exec uvicorn pulsefeed.api:create_app --factory --host 0.0.0.0 --port 8000 \
"]

HEALTHCHECK --interval=15s --timeout=3s --start-period=20s --retries=3 \
  CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://localhost:8000/healthz', timeout=2).status==200 else 1)"
