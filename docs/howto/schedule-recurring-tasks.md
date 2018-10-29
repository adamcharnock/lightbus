# How to schedule recurring tasks

Recurring tasks can be scheduled in two ways:

* The `@bus.client.every()` decorator – Will execute a function or coroutine at a given interval
* The `@bus.client.schedule()` decorator – Similar to `every()`, but takes complex schedules as provided by the [schedule] library.

## Simple recurring tasks using `@bus.client.every()`

Lightbus natively supports simple recurring tasks using the `@bus.client.every()` decorator:

```python3
# bus.py
import lightbus

bus = lightbus.create()

@bus.client.every(seconds=1)
def do_it():
    print("Hello!")

```

The interval can be specified using the `seconds`, `minutes`, `hours`, and `days` keys.
Pass `also_run_immediately=True` to execute the function/coroutine immediately, as well as 
at the given interval.

## Complex schedules using `@bus.client.schedule()`

Lightbus also supports using schedules specified using 
the [schedule] library. This allows for schedules 
such as 'every Monday at 1am', rather than simple intervals.
For example:

```python3
import lightbus
import schedule

bus = lightbus.create()

# Run the task every 1-3 seconds, varying randomly
@bus.client.schedule(schedule.every(1).to(3).seconds)
def do_it():
    print("Hello using schedule library")

```

[schedule]: https://github.com/dbader/schedule
