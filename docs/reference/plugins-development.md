Plugins provide hooks into Lightbus' inner workings.

For example, the bundled [StatePlugin]
 hooks into the `before_server_start` and `after_server_stopped` hooks. 
The plugin uses these hooks to bus events indicating the state of the worker. 
A `internal.state.server_started` event indicates a worker has started, and a 
`internal.state.server_stopped` event indicates a worker has stopped. Consuming 
these events will provide a picture of the current state of workers on the bus.

## Example plugin

Let's create a simple plugin which will print a configurable message to the 
console every time a message is sent.

This requires three steps:

1. Define the plugin
1. Make Lightbus aware of the plugin
1. Configure the plugin

### 1. Define the plugin

Create the following in a python module (we'll assume it is at `my_project.greeting_plugin`):

```python3
from lightbus.plugins import LightbusPlugin


class GreetingPlugin(LightbusPlugin):
    """Print a greeting after every event it sent"""
    
    priority = 200

    def __init__(self, greeting: str):
        self.greeting = greeting

    @classmethod
    def from_config(cls, config: "Config", greeting: str = "Hello world!"):
        # The arguments to this method define the configuration options which 
        # can be set in the bus' yaml configuration (see below)
        return cls(greeting=greeting)

    async def after_event_sent(self, *, event_message, client):
        # Print a simple greeting after an event is sent
        print(self.greeting)
```

### 2. Make Lightbus aware of the plugin

Lightbus is made aware of the plugin via an entrypoint in you're project's `setup.py` file:

```python
# Your setup.py file
from setuptools import setup, find_packages

setup(
    name='My Project',
    version='0.1',
    packages=find_packages(),
    entry_points={
        "lightbus_plugins": [
            # `greeting_plugin` defines the plugin name in the config
            # `my_project.greeting_plugin` is your plugin's python module
            # `GreetingPlugin` is your plugins class name
            "greeting_plugin = my_project.greeting_plugin:GreetingPlugin",
        ],
    }
)
```

Once you've made this change (or for subsequent modifications) you will need to run:

    python setup.py develop
    
This will setup the `entry_points` you have specified

### 3. Configure the plugin

You can configure your plugin in your bus' configuration YAML file 
(see the [configuration reference](configuration.md)). For example:

```yaml
bus:
  ...

apis:
  ...

plugins:
  greeting_plugin:
    
    # The 'enabled' configuration is available for all plugins, 
    # if set to 'false' the plugin will not be loaded.
    # Default is false
    enabled: true
    
    # Lightbus is aware of our `greeting` option as it reads it 
    # from our `from_config()` method above.
    # If we omit this it will have the default value of "Hello world!"
    greeting: "Hello world, I sent an event!"
```

## Plugin hooks/methods

The following hooks are available. Each of these should be implemented 
as an asynchronous method on your plugin class. 

**For full reference see the [LightbusPlugin class]**

* `before_parse_args`
* `receive_args`
* `before_server_start`
* `after_server_stopped`
* `before_rpc_call`
* `after_rpc_call`
* `before_rpc_execution`
* `after_rpc_execution`
* `before_event_sent`
* `after_event_sent`
* `before_event_execution`
* `after_event_execution`
* `exception`

[StatePlugin]: https://github.com/adamcharnock/lightbus/blob/master/lightbus/plugins/state.py
[LightbusPlugin class]: https://github.com/adamcharnock/lightbus/blob/master/lightbus/plugins/__init__.py
