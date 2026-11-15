# syntax=docker/dockerfile:1.7
#
# One image for every backend role (api, ingestor, processor, executor, agents, migrate).
# The role is chosen by the command, so there is one artifact to build, scan and promote.

# --- Build stage: resolve and install dependencies into a virtualenv -----------------
FROM python:3.13-slim AS builder
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    PIP_DISABLE_PIP_VERSION_CHECK=1
# Behind a TLS-intercepting proxy, pass its CA as an optional build secret:
#   docker build --secret id=ca,src=/path/to/ca.pem ...
# Without the secret these steps use the image's normal certificate store.
RUN --mount=type=secret,id=ca,required=false \
    if [ -f /run/secrets/ca ]; then export PIP_CERT=/run/secrets/ca; fi; \
    pip install --no-cache-dir "uv>=0.11,<0.12"
WORKDIR /app

# Dependencies first, from the lockfile only. This layer is rebuilt when the lockfile
# changes, not on every source edit, which is most of what makes rebuilds fast.
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv --mount=type=secret,id=ca,required=false \
    if [ -f /run/secrets/ca ]; then export SSL_CERT_FILE=/run/secrets/ca; fi; \
    uv sync --frozen --no-dev --no-install-project

COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv --mount=type=secret,id=ca,required=false \
    if [ -f /run/secrets/ca ]; then export SSL_CERT_FILE=/run/secrets/ca; fi; \
    uv sync --frozen --no-dev --no-editable

# --- Runtime stage: no compilers, no package manager, no root -------------------------
FROM python:3.13-slim AS runtime
RUN groupadd --system --gid 10001 app \
 && useradd --system --uid 10001 --gid app --home-dir /app --no-create-home app
WORKDIR /app
COPY --from=builder /app/.venv /app/.venv
COPY alembic.ini ./
COPY alembic ./alembic
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    LOG_JSON=true \
    EMBEDDED_WORKERS=none
USER 10001:10001
EXPOSE 8000 9100
ENTRYPOINT ["bazaarpulse"]
CMD ["api"]
