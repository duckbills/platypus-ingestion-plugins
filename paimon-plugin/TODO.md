# Paimon Plugin TODO

## High Priority

- [ ] Implement S3 checkpoint persistence for production crash recovery
    - Current implementation uses in-memory `AtomicReference<Long> lastCheckpointId`
    - Need persistent storage in S3 for coordinator crash recovery

## Medium Priority

- [ ] Implement dead letter queue for permanently failed records
- [ ] Add basic metrics (queue size, error counts, checkpoint lag)
- [ ] Performance tuning and optimization

## Low Priority

- [ ] Proper JSON conversion for complex types
- [ ] Configuration validation
- [ ] Graceful shutdown improvements