import argparse
import asyncio
import contextlib
import logging
from pathlib import Path

from pyqueue.broker import Broker
from pyqueue.handler import RequestHandler
from pyqueue.server import Server

logger = logging.getLogger("pyqueue")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="pyqueue", description="Run a PyQueue broker.")
    parser.add_argument("--data-dir", type=Path, default=Path("data"), help="default: ./data")
    parser.add_argument("--host", default="127.0.0.1", help="default: 127.0.0.1")
    parser.add_argument("--port", type=int, default=9092, help="default: 9092")
    return parser.parse_args()


async def serve(broker: Broker, host: str, port: int) -> None:
    # Returns after the server has stopped, so the broker closes only after that.
    await Server(RequestHandler(broker), host, port).serve_forever()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    # The broker is opened before the event loop starts and closed after it stops.
    with Broker(args.data_dir) as broker:
        logger.info("data directory %s, topics: %s", args.data_dir.resolve(), broker.topics())
        with contextlib.suppress(KeyboardInterrupt):
            asyncio.run(serve(broker, args.host, args.port))
    logger.info("stopped")


if __name__ == "__main__":
    main()
