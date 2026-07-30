"""Discover serial and BLE candidates; TCP targets are always explicit."""

import asyncio

from meshcore_sdk import DiscoveryError, discover_ble, discover_serial


async def main() -> None:
    for device in await discover_serial():
        print(device)
    try:
        for device in await discover_ble(timeout=3.0):
            print(device)
    except DiscoveryError as error:
        print(f"BLE discovery unavailable: {error}")


if __name__ == "__main__":
    asyncio.run(main())
