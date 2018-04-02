import asyncio


async def main():
    task = asyncio.ensure_future(asyncio.sleep(1))
    task.cancel()
    await task

loop = asyncio.get_event_loop()
loop.run_until_complete(main())
loop.close()
