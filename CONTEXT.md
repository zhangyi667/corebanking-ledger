# Ledger

Downstream consumer of posting events. Currently log-only — reads
`posting.transaction.received` from Kafka and writes structured log lines.
Future scope: apply postings against accounts, emit balance change events.

## Language

**Received Event**: A `posting.transaction.received` Kafka envelope consumed by the ledger. Carries the original `Posting` payload plus `idempotencyKey` and `receivedAt`. The ledger has not applied anything when it sees this.
_Avoid_: Applied event, posting event

**Consumer Group**: The Kafka consumer group `corebanking-ledger`. Partitions assigned across replicas; 6 partitions, default 3 replicas → 2 partitions per replica.
_Avoid_: Group, listener group

**Dead-Letter Topic**: Per-source-topic DLQ named `<topic>.dlq`. Receives the raw record bytes after deserialization failures or N consumer retries. No payload contract — just preserved bytes plus original partition.
_Avoid_: Failure topic, DLT (different concept in Spring Kafka), error queue
