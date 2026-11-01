import asyncio
import contextlib
import logging

from pyqueue.server import Server


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(Server().serve_forever())


if __name__ == "__main__":
    main()
