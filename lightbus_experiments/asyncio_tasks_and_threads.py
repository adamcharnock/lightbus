import asyncio
from functools import partial
from threading import Thread
from time import sleep


def setter(f: asyncio.Future):
    sleep(1)
    f.get_loop().call_soon_threadsafe(
        partial(f.set_result, None)
    )
    print("Setting done")


async def main():
    f = asyncio.Future()
    setter_thread = Thread(target=setter, args=[f])

    setter_thread.start()

    await f
    print("Waiting complete")

    setter_thread.join()


if __name__ == '__main__':
    asyncio.run(main())
