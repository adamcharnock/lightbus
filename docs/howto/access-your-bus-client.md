# How to access your bus client

You create your bus client in your bus module
(typically called `bus.py`) with the line:

```python3
# Creating your bus client in your bus.py file
import lightbus

bus = lightbus.create()
```

However, you will often need to make use of your bus client
in other areas of your codebase. For example, you may need to
fire an event when a web form is submitted.

You can access the bus client in two ways.

## Method 1: Direct import (recommended)

The first approach is to import your bus client directly from
your bus module, in the say way you would import anything else
in your codebase:

```python3
# For example
from bus import bus

# ...or if your service has its own package
from my_service.bus import bus
```

You should use this approach in code which is specific to your
service (i.e. non-shared/non-library code). This approach is more
explicit (good), but hard codes the path to your bus module (bad for shared code).

## Method 2: `get_bus()`

The second approach uses the `lightbus.get_bus()` function. This will
use the [module loading configuration] to determine the bus module location.
If the bus module has already been imported, then the module's
`bus` attribute will simply be returned. Otherwise the bus module will be
imported first.

```python3
# Anywhere in your codebase
import lightbus

bus = lightbus.get_bus()
```

This approach is best suited to when you do not know where your bus module
will be at runtime. This could be the case when:

* Writing shared code which is used by multiple services
* Writing a third party library


[module loading configuration]: /reference/configuration.md#1-module-loading
