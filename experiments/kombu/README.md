Kombu Simple Test
=================

Sending 1kb messages, acknowledged, no returned response

    $ python hello_publisher.py 10000
    Time per put: 0.12
    
    $ python hello_consumer.py 10000
    Time per get: 0.22
