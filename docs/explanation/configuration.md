# Configuration

As discussed in the [configuration reference], Lightbus has three stages of configuration:

1. Module loading
2. Service-level configuration
3. Global bus configuration 

See the [configuration reference] for details on how this works in practice. Here we will discuss 
the reasoning behind this system.

## 1. Module loading

Lightbus needs to know how to bootstrap itself. It needs to know where to start. 
This module loading step is how we provide Lightbus with this information, via the 
`LIGHTBUS_MODULE` environment variable.

The module loading was inspired by [Django]'s `DJANGO_SETTINGS_MODULE` environment variable.

`LIGHTBUS_MODULE` has a sensible default of `bus` in the same way that `DJANGO_SETTINGS_MODULE` 
has a sensible default of `settings`. This default will work in many scenarios, but may also 
need to be customised depending on one's project structure.

# 2. Service-level configuration

Some configuration must by its nature be specific to a service, and not global to the 
entire bus. These options are broadly:

1. Configuration which distinguishes this service from any other service (`service_name` / `process_name`)
1. Configuration related to the specific deployment of this service (`features`)
1. A pointer to the global bus configuration

# 3. Global bus configuration

The global bus configuration provides the bulk of the Lightbus options. 
This configuration should be consistent across all Lightbus clients.

But what is the reasoning here? The reasoning is that the bus is a globally shared 
resource, therefore everything that uses the bus is going to need a follow a 
common configuration in order to function.

For example, consider the case where one bus client is 
configured to connect to redis server A for the `customers` API, 
and another bus client is configured to connect to redis server B for the same API.
What will happen?

The result will be that you will have effectively created a network partition. Each bus 
client will operate in total ignorance of each other. Events could be lost or ignored, and 
RPCs may never be processed.

**Some** configuration **must** therefore be common to all clients, and that is 
what the global configuration provides.

### Configuration loading over HTTP(S)

To this end, Lightbus supports loading configuration over HTTP(S). **The intention is not
for you to host you Lightbus configuration on the public internet!**. Rather, and if you wish,
you may find is useful to host your configuration on an internal-only endpoint.

Alternatively, you may decide to ensure your build/deploy process distributes a copy 
of the global configuration file to every service. 
[The discussion on monorepositories in Architecture Tips](architecture-tips.md#use-a-monorepository) is relevant here.

[configuration reference]: ../reference/configuration.md
[Django]: https://www.djangoproject.com/
