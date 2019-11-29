# Testing & Mocking

Lightbus provides utilities to make testing easier. 
These utilities allow you to:

* Ensure only specific events & RPCs were fired
* Access the sent messages
* Mock RPC responses

## Mocking with events

```python
from lightbus.utilities.testing import BusMocker

from bus import bus

def test_firing_event():
    with BusMocker(bus) as bus_mock:
        # Setup the mocker to expect the auth.user_created was fired.
        # An error will be raised if the tested code fires any other events
        bus_mock.mock_event_firing("auth.user_created")
        
        # Run the code to be tested
        bus.auth.user_created.fire(field="x")
        
        # Check the event was fired once
        bus_mock.assert_events_fired("auth.user_created", times=1)
        
        # Get the fired event message
        message = bus_mock.get_event_messages("auth.user_created")[0]
        assert message.kwargs == {"username": "sarahjane"}
```


## Mocking with RPCs

```python
from lightbus.utilities.testing import BusMocker

from bus import bus

def test_calling_rpc():
    with BusMocker(bus) as bus_mock:
        # Setup the mocker to expect the auth.user_created was fired.
        # An error will be raised if the tested code calls any other RPCs
        bus_mock.mock_rpc_call("auth.check_password", result=True)
        
        # Run the code to be tested
        bus.auth.check_password(username="sarahjane", password="secret")
        
        # Check the event was fired once
        bus_mock.assert_rpc_called("auth.check_password", times=1)
        
        # Get the fired RPC
        message = bus_mock.get_rpc_messages("auth.check_password")[0]
        assert message.kwargs == {"username": "sarahjane", "password": "secret"}
```

## Allowing arbitrary events and RPCs

By default the mocker will raise an error if any event or RPC is fired or called which has 
not be setup using the `mock_event_firing` / `mock_rpc_call` methods.

You can disable this and therefore allow any events or RPCs to the fired/called by using 
`BusMocker(bus, require_mocking=False)`.

For example, the following will result in no errors even though we have not 
setup any mocks:

```python
from lightbus.utilities.testing import BusMocker

from bus import bus

def test_permissive_mocking():
    with BusMocker(bus, require_mocking=False) as bus_mock:
        # Neither will cause an error despite the lack of mocking setup.
        # This is because we have set require_mocking=False
        bus.auth.user_created.fire(field="x")
        bus.auth.check_password(username="sarahjane", password="secret")
```

## The `@bus_mocker` decorator

You can also access the bus mocker using the `@bus_mocker` decorator. We can rewrite 
our earlier event example (above) as follows:


```python
from lightbus.utilities.testing import bus_mocker

from bus import bus

@bus_mocker(bus)
def test_firing_event(bus_mock):
    # Setup the mocker to expect the auth.user_created was fired.
    # An error will be raised if the tested code fires any other events
    bus_mock.mock_event_firing("auth.user_created")
    
    # Run the code to be tested
    bus.auth.user_created.fire(field="x")
    
    # Check the event was fired once
    bus_mock.assert_events_fired("auth.user_created", times=1)
    
    # Get the fired event message
    message = bus_mock.get_event_messages("auth.user_created")[0]
    assert message.kwargs == {"username": "sarahjane"}
```

## Testing in Django

You can use the mocker in your Django tests just as shown above.
For example, we can access the mocker using the context manager:

```python
from django.test import TestCase
from lightbus.utilities.testing import BusMocker

from bus import bus


class ExampleTestCase(TestCase):

    def test_something(self):
        with BusMocker(bus, require_mocking=False) as bus_mock:
            ...
```

Or we can use the `@bus_mocker` decorator:


```python
from django.test import TestCase
from lightbus.utilities.testing import bus_mocker

from bus import bus


class ExampleTestCase(TestCase):
    
    @bus_mocker(bus)
    def test_something(self, bus_mock):
        ...
```
