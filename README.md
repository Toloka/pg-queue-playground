# PostgreSQL Queue Playground

Here there are a couple of stress tests that compare throughput of different implementations of the Polling Publisher
pattern.

The goal is to show performance degradation due to a long-running transaction and how the TRUNCATE operation can solve
this problem.

Requirements
--------------

- Java 21+
- Docker (2 CPU, 2 GB RAM)

Useful Links
--------------

- [Transactional Outbox design pattern](https://microservices.io/patterns/data/transactional-outbox.html)
- [Polling Publisher design pattern](https://microservices.io/patterns/data/polling-publisher.html)

License
-------
© TOLOKA AI BV, 2024. Licensed under the Apache License, Version 2.0. See LICENSE file for more details.