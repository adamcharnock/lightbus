# Architecture tips

These tips draw some core concepts from
the [Domain Driven Design] and [CQRS] patterns. We only
scape the surface of these patterns here, but the ideas covered
below are the ones the Lightbus maintainers found most useful when using Lightbus.

There is a lot more to be said on each point. The intent
here is to provide a digestible starting point.

## Use common data structures

Create structures which represent the data you wish to transmit.
Both [NamedTuples] and [dataclasses] work well for this purpose.

For example:

```python3
from typing import NamedTuple

class Customer(NamedTuple):
    name: str
    email: str
    age: int
```

You can then take advantage of Lightbus' schema checking and data casting:

```python3
import lightbus

class CustomerApi(lightbus.Api):

    class Meta:
        name = "customer"

    def create(customer: Customer):
        # Lightbus will ensure the incoming data conforms to
        # Customer, and will cast the data to be a Customer object
        ...
```

You would call the `customer.create()` API as follows:

```python3
import lightbus
bus = lightbus.create()

bus.customer.create(
    customer=Customer(name="Joe", email="joe@gmail.com", age=54)
)
```

This provides a standard way of communicating shared concepts across
your services.

!!! note

    You can still send and receive events even if you do
    not have the data strucuture available. To send you can simply
    use a dictionary:

    ```python3
    bus.customer.create(
        customer={"name": "Joe", "email": "joe@gmail.com", "age": 54}
    )
    ```

    Likewise, an RPC or event listener without type hints will simply
    receive a dictionary.

See also [use a monorepository](#use-a-monorepository).

## Decide on boundaries

Your services will likely use an assortment of entities. It is common
for these to map to database tables or [ORM] classes. For example,
you may have `Customer`, `Address`, `Order`, and `OrderLine`  entities (and probably
many more). You will need to pass these between your services,
but how should you structure them in order to do so? Do pass them
individually, or combine them into a hierarchy? Do you sometimes
omit some fields for brevity, or not?

Take the following events for example:

* `customer.signup(customer)` – Does the `customer` object include the address(es)?
* `order.shipped(order)` – Does the `order` object contain each line item?
* `order.line_item_deleted(order, line_item)` – Do we need to pass `line_item` here, or should the line items be included within `order`?

This is a simple example with only four entities. In a real-world
system it is very easy to end up passing around data in
forms which become increasingly bloated, inconsistent, and complex.

**You can mitigate this by grouping your entities together in a way that
makes logical sense for your situation**. These are your *aggregates*.
Decide on these aggregates in advance, and stick to it.

For example, we may group the `Customer`, `Address`, `Order`, and `OrderLine`
entities as follows:

* A `Customer` aggregate. Each `Customer` contains an `address` field, which is an `Address` entity.
* An `Order` aggregate. Each `Order` contains a `line_items` field, which is a list of `OrderLine` entities.

The `Customer` and `Order` aggregates also will likely contain other fields not listed above (such as name, 
email, or quantity).

**You should only ever pass around the top level aggregates.**

Additionally:

* Identify aggregates with UUIDs
* Do not enforce foreign keys between aggregates. Your application code
  will need to deal with inconsistencies gracefully (likely in it's user interface).
* It is likely still a good idea to use sequential integer primary keys in your RDBMS
* Do **not** share database-level sequential integer primary keys with other servces

## Writes are special

Writes are inherently different to reads in a distributed system.
Reading data is generally straightforward, whereas data modifications
need to be propagated to all concerned services.

**Reading data** looks broadly like this:

1. Read data from storage (disk, ORM, memory etc)
1. Perhaps transform the data
1. Display the data, or send it somewhere

An **initial attempt at writing data** may look like this:

1. Write the data locally
1. Broadcast the change (Lightbus event or RPC)
1. Other services receive & apply the change

The downside of this approach is that you have two write paths (writing locally and broadcasting the change). 
If either of these fails then your services will have an inconsistent view of the world.

A more **CQRS-based approach to writing data** looks like this:

1. Broadcast the change (Lightbus event)
1. Services (including the broadcasting service) receive & apply the change

The benefit of this approach is that either the change happens or it does not. Additionally 
all applications share the same write path, which reduces overall complexity. A downside 
is that changes can take a moment to be applied,
so changes made by a user may not be immediately visible.

## Use a monorepository

A monorepository is a repository which stores *all* your services, rather than having 
one repository per service. There are pros and cons to this approach and you should 
perform your own research. The relevant benefits to Lightbus (and distributed architectures in 
general) are:

* **Atomic commits across multiple services** – useful when a change to one service affects another.
* **Easier sharing of data structures** – this provides a shared language across all of your services. 
  These data structures can be used as type hints on your events and RPCs, which Lightbus will 
  automatically cast data into.
* **Sharing of APIs** – in some cases you may wish for multiple services fire events on the same API. 
  In this case each of these services will need to have access to the API class. 
  (See [API registration & authoritative/non-authoritative APIs](apis.md#api-registration-authoritativenon-authoritative-apis))
* **Sharing of global bus configuration** – similar to sharing data structures, your global bus configuration 
  YAML file can likewise be available to all services.
