# How to write idempotent event handlers

An idempotent function is a function which, when called multiple times with the same parameters, will not result in any 
change after the first call. As [Wikipedia puts it](https://en.wikipedia.org/wiki/Idempotence):

> Idempotence is the property of certain operations in mathematics and computer science 
> whereby they can be applied multiple times without changing the result beyond the initial application

## Why is idempotency useful?

Consider that Lightbus events will be delivered [at least once](../explanation/events.md#at-least-once-semantics). 
This is in comparison to RPCs which are called [at most once](../explanation/rpcs.md#at-most-once-semantics).
This is not Lightbus trying to be awkward, rather it is because it is theoretically impossible to ensure exactly-once 
delivery.

Lightbus therefore guarantees you that events will always arrive, but with the trade-off that sometimes the event 
may arrive multiple times. **In the wost case, your event handlers will be executed multiple times.**

In some cases this will not cause a problem. For example, an event handler which performs a 
simple **update** to a record with a specific ID. Executing this update a second time will leave the 
record in the same state it was in after the first execution.

However, an event handler which **creates** a new record will face a problem. If the event handler is executed twice
then two records will be created, even though the event was identical in both cases.

## Ensuring idempotency for database operations

A simple and reliable way to ensure idempotency is through the use of *UUIDs* and *upserts*.

A UUID is a standard way of producing a globally unique ID. An upsert is a combination of 
an 'update' and an 'insert' which says: *'Insert this record, but if you find a duplicate then update it instead'. 

Take the following event handler as an example:

```python
# BAD, not idempotent
def handle_page_view(event, url):
    db.execute("INSERT INTO views (url)")
```

This is not an idempotent event handler because multiple invocations for the same event will 
result in multiple records being created.

An idempotent version of the above can be rewritten as:

```python
# GOOD, idempotent
def handle_page_view(event, uuid, url):
    try:
        # Try to perform an insert first (this should normally work fine)
        db.execute("INSERT INTO views (%s, %s)", [uuid, url])
    except IntegrityError:
        # UUID already exists, so do an update
        db.execute("UPDATE views SET url = %s WHERE view_uuid = %s", [url, uuid])
```

This handler can be executed any number of times for the same parameters without ever 
creating duplicate records. It is idempotent.

Note that the `views` table must include a `UUID` column which is `UNIQUE`, otherwise an error 
would not be raised. Additionally, the code which fires the event must now provide a value 
for the UUID parameter.

Many databases and frameworks provide direct support for performing upsert operations 
which will make the above simpler (and therefore less error prone).

### Upserts in Django

Django provides the [update_or_create()](https://docs.djangoproject.com/en/latest/ref/models/querysets/#update-or-create)
method which serves exactly this purpose:

```python
# Idempotent
def handle_page_view(event, uuid, url):
    Views.objects.update_or_create(uuid=uuid, defaults={'url': url})
```

### Upserts in SQL

Many relational databases can also perform upsert in a single SQL query:

```postgresql
INSERT INTO views (uuid, url) VALUES ('uuid-here', 'http://lightbus.org') 
ON CONFLICT (uuid) 
DO UPDATE SET url = 'http://lightbus.org';
```

## Ensuring idempotency in other cases

There are additional areas where idempotency can become relevant:

* Event handlers which call external APIs
* Event handlers which send email

Handling these cases is beyond the scope of this documentation. Rest 
APIs which support idempotency keys will likely help here [^1]. 
You may also decide that multiple invocations are acceptable in some situations
as long as they are sufficiently rare (for example, sending emails).


[^1]: Blog post from Stripe: [Designing robust and predictable APIs with idempotency](https://stripe.com/gb/blog/idempotency)
