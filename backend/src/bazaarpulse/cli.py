"""Command line entry points: one image, many roles.

bazaarpulse api                 the HTTP/WebSocket API (plus every worker when EMBEDDED_WORKERS=all)
bazaarpulse worker <name>       one worker: ingestor | processor | executor | agents
bazaarpulse migrate             apply database migrations
bazaarpulse seed                load reference data
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import signal
from pathlib import Path

from bazaarpulse.config import get_settings
from bazaarpulse.container import WORKERS, build_container, seed, supervise
from bazaarpulse.logging import configure_logging, get_logger

log = get_logger(__name__)
BACKEND_DIR = Path(__file__).resolve().parents[2]


def migrate() -> None:
    from alembic import command
    from alembic.config import Config

    # In a source checkout the migrations sit next to src/; in the container image they are copied to /app.
    root = next((p for p in (BACKEND_DIR, Path.cwd()) if (p / "alembic.ini").exists()), BACKEND_DIR)
    config = Config(str(root / "alembic.ini"))
    config.set_main_option("script_location", str(root / "alembic"))
    command.upgrade(config, "head")


async def _worker(name: str, metrics_port: int) -> None:
    from prometheus_client import start_http_server

    settings = get_settings()
    configure_logging(settings.log_level, json=settings.log_json)
    container = await build_container(settings)
    if metrics_port:
        start_http_server(metrics_port)  # workers have no HTTP API, so they expose /metrics on their own port
    task = asyncio.create_task(supervise(name, container))

    # SIGTERM is how Kubernetes asks a pod to stop: finish cleanly instead of being killed mid-batch.
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, task.cancel)
    log.info("worker.started", worker=name, instance=container.instance)
    with contextlib.suppress(asyncio.CancelledError):
        await task
    await container.aclose()
    log.info("worker.stopped", worker=name)


async def _seed() -> None:
    container = await build_container(get_settings())
    await seed(container)
    await container.aclose()


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="bazaarpulse", description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    commands = parser.add_subparsers(dest="command", required=True)
    api = commands.add_parser("api", help="run the API server")
    api.add_argument("--host", default="0.0.0.0")  # noqa: S104 - a server in a container must listen on all interfaces
    api.add_argument("--port", type=int, default=8000)
    api.add_argument("--reload", action="store_true")
    worker = commands.add_parser("worker", help="run one background worker")
    worker.add_argument("name", choices=sorted(WORKERS))
    worker.add_argument("--metrics-port", type=int, default=9100)
    commands.add_parser("migrate", help="apply database migrations")
    commands.add_parser("seed", help="load reference data")
    args = parser.parse_args()

    if args.command == "api":
        import uvicorn

        uvicorn.run("bazaarpulse.api.app:create_app", factory=True, host=args.host, port=args.port, reload=args.reload)
    elif args.command == "worker":
        asyncio.run(_worker(args.name, args.metrics_port))
    elif args.command == "migrate":
        migrate()
    else:
        asyncio.run(_seed())


if __name__ == "__main__":
    main()
