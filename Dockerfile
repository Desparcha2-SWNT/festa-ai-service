# syntax=docker/dockerfile:1.7
# Build:  docker build --build-context contracts=../festa-infra/contracts -t festa-ai-service .
# (docker compose en festa-infra ya pasa el contexto "contracts").

FROM python:3.12-slim AS test
WORKDIR /build
COPY requirements.txt requirements-dev.txt ./
RUN --mount=type=cache,target=/root/.cache/pip pip install -r requirements-dev.txt
COPY . .
COPY --from=contracts . /contracts
# Las pruebas que no necesitan Mongo corren en el build: si fallan, no hay imagen.
RUN FESTA_CONTRACTS_DIR=/contracts python -m pytest -q && touch /build/.tests-ok

FROM python:3.12-slim
RUN useradd --system --create-home festa
WORKDIR /app
COPY requirements.txt ./
RUN --mount=type=cache,target=/root/.cache/pip pip install -r requirements.txt
COPY --from=test /build/.tests-ok /tmp/.tests-ok
COPY app ./app
# El inbox valida los mensajes contra estos esquemas en runtime.
COPY --from=contracts . ./contracts
ENV FESTA_CONTRACTS_DIR=/app/contracts PYTHONUNBUFFERED=1
USER festa
EXPOSE 8086
HEALTHCHECK --interval=15s --timeout=5s --start-period=30s --retries=5 \
  CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://localhost:8086/health').status==200 else 1)"
CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8086} --proxy-headers --forwarded-allow-ips='*'"]
