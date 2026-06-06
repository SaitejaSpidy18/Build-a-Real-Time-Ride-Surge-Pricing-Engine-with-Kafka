# Architectural Decisions

## Partitioning Strategy

`ride-requests` has 6 partitions because there are 6 city zones, so the setup gives each zone enough room for parallel processing while keeping the design simple. With 2 partitions, different zones would be forced to share partitions, which reduces parallelism and can create bottlenecks; with 24 partitions, the system would carry extra overhead and complexity without much real benefit for only 6 zones.

## Consumer Group Design

`pricing-engine` and `analytics-recorder` are separate consumer groups because they do different jobs and should each receive every `ride-requests` message independently. The pricing engine needs to react in real time and may emit alerts, while the analytics recorder needs to persist zone statistics to the database; separating them prevents one from stealing messages from the other and lets each scale or fail independently.

If they were in the same consumer group, Kafka would split messages between them instead of giving both full access, which would break the workflow.

## Offset Commit Strategy

[Explain your choice of auto vs manual commits for each consumer.]

For both consumers, **manual commits** are the safer choice because each message should be acknowledged only after the work is actually finished. In `pricing-engine`, that means committing after the surge calculation and alert publish succeed; in `analytics-recorder`, it means committing after the PostgreSQL write succeeds.

Auto-commit is simpler, but it can mark a message as processed before the consumer has really finished its job, which risks data loss if the process crashes. Manual commits give you stronger at-least-once reliability, which fits a real-time pipeline better.
