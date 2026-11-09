"""@YOKE_PROGRAM@: a Yoke Plugin. Its Manifest is kept beside the source, as @YOKE_ID@.yaml, and is what
`main.py --manifest` prints; the continuous integration of this project compares the two."""

import asyncio
import signal
import sys

from yoke_sdk import plugin


def declaration():
    """What this Plugin says about itself: its identity, and whatever it publishes and answers."""
    return plugin.Declaration(id="@YOKE_ID@")


async def run():
    unit = await plugin.start(declaration())
    await unit.health(0, "ready")
    stopping = asyncio.Event()
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, stopping.set)
    asked = asyncio.create_task(stopping.wait())
    while True:
        told = asyncio.create_task(unit.next())
        done, _ = await asyncio.wait({told, asked}, return_when=asyncio.FIRST_COMPLETED)
        if asked in done:
            told.cancel()
            await unit.close()
            return 0
        event = told.result()
        if event is None or isinstance(event, plugin.Ended):
            return 0


if __name__ == "__main__":
    if sys.argv[1:] == ["--manifest"]:
        sys.stdout.write(declaration().manifest())
        sys.exit(0)
    sys.exit(asyncio.run(run()))
