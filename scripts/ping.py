"""Live check against a running PyQueue server: create a topic, produce, fetch.

Run the server first (`uv run python -m pyqueue`), then `uv run python scripts/ping.py`.
Running it again appends more records to the same topic.
"""

import asyncio

from pyqueue.client import Client, RequestFailedError
from pyqueue.messages import ErrorCode, ProduceRecord

TOPIC = "ping"


async def main() -> None:
    async with Client() as client:
        try:
            await client.create_topic(TOPIC, 2)
            print(f"created topic {TOPIC!r}")
        except RequestFailedError as exc:
            if exc.error is not ErrorCode.TOPIC_ALREADY_EXISTS:
                raise
            print(f"topic {TOPIC!r} already exists")
        print("topics:", await client.metadata())

        # Three concurrent produce requests on one connection (pipelining).
        base_offsets = await asyncio.gather(
            *(
                client.produce(TOPIC, 0, [ProduceRecord(f"message {i}".encode(), b"key")])
                for i in range(3)
            )
        )
        print("base offsets:", sorted(base_offsets))

        records, high_watermark = await client.fetch(TOPIC, 0, 0)
        print(f"partition 0 has {high_watermark} records, last ones:")
        for record in records[-3:]:
            print(f"  offset={record.offset} key={record.key!r} value={record.value!r}")


if __name__ == "__main__":
    asyncio.run(main())
