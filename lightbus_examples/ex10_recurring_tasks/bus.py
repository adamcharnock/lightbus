import lightbus

bus = lightbus.create()

# Simple


@bus.client.every(seconds=1)
def do_it():
    print("Simple hello")


# Using the schedule library
import schedule

# Run the task every 1-3 seconds, varying randomly
@bus.client.schedule(schedule.every(1).to(3).seconds)
def do_it():
    print("Hello using schedule library")
