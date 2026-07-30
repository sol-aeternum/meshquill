"""Consume independent bounded message and event subscriptions."""

import asyncio

from meshcore_sdk import Client


async def main() -> None:
    async with await Client.demo() as mesh:
        messages = mesh.messages()
        events = mesh.events()
        initial = [await events.__anext__(), await events.__anext__()]
        print([event.kind for event in initial])

        async def print_first_message() -> None:
            async for message in messages:
                print(f"{message.sender}: {message.text}")
                break

        receiver = asyncio.create_task(print_first_message())
        event, fetched_message = await asyncio.gather(
            events.__anext__(), mesh.fetch_queued_message()
        )
        await receiver
        print(event.kind)
        print(fetched_message)


if __name__ == "__main__":
    asyncio.run(main())
