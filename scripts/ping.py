"""Send a few concurrent requests to a running PyQueue server and print the responses."""

import asyncio

from pyqueue.client import Client
from pyqueue.protocol import RequestType


async def main() -> None:
    async with Client() as client:
        responses = await asyncio.gather(
            *(client.request(RequestType.METADATA, f"ping {i}".encode()) for i in range(3))
        )
    for response in responses:
        print(response)


if __name__ == "__main__":
    asyncio.run(main())
