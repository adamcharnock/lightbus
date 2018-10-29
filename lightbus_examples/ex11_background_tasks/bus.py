# bus.py
import asyncio
import lightbus

bus = lightbus.create()


async def my_background_task():
    while True:
        await asyncio.sleep(1)
        print("Hello!")


@bus.client.on_start()
def on_startup(**kwargs):
    bus.client.add_background_task(my_background_task())
