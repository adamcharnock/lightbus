# How to run background tasks

Sometimes you may wish to run arbitrary `asyncio` tasks in the background of the 
`lightbus run` process. You can set these up in your `bus.py` file:

```python3
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
```

Important points to note are:

* The background task will be automatically cancelled when the bus is closed.
* Any errors in the background task will be bubbled up and cause the 
  Lightbus process to exit. If this is not desired you can implement 
  your own try/except handling within the function being executed.

!!! note

    If you wish to schedule a recurring task then you should probably use 
    `@bus.client.every()` or `@bus.client.schedule()`. See 
    [how to schedule recurring tasks](run-background-tasks.md).
