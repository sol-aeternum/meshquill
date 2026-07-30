"""Deterministic send/ACK quickstart; replace Client.demo() with Client.auto()."""

import asyncio

from meshcore_sdk import Client


async def main() -> None:
    async with await Client.demo() as mesh:
        contacts = await mesh.list_contacts()
        print([contact.name for contact in contacts])
        receipt = await mesh.send("Alice", "Hello from Python")
        ack = await mesh.wait_for_ack(receipt)
        print(f"acknowledged by {ack.code_hex}")


if __name__ == "__main__":
    asyncio.run(main())
