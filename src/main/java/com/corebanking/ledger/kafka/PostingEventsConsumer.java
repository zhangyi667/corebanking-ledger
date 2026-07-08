package com.corebanking.ledger.kafka;

import com.corebanking.ledger.dedup.ProcessedEventTracker;
import com.corebanking.ledger.events.EventEnvelope;
import com.corebanking.ledger.events.PostingTransactionReceived;
import tools.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PostingEventsConsumer {

    private static final Logger log = LoggerFactory.getLogger(PostingEventsConsumer.class);

    private final ObjectMapper objectMapper;
    private final ProcessedEventTracker dedup;

    public PostingEventsConsumer(ObjectMapper objectMapper, ProcessedEventTracker dedup) {
        this.objectMapper = objectMapper;
        this.dedup = dedup;
    }

    @KafkaListener(
            topics = "${app.kafka.topics.posting-transaction}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "postingEventsListenerContainerFactory"
    )
    public void onPostingTransactionReceived(EventEnvelope envelope) {
        // TODO: call account-service to apply the leg mutations once the money-mover ships.
        PostingTransactionReceived payload =
                objectMapper.convertValue(envelope.payload(), PostingTransactionReceived.class);

        if (!dedup.markSeen(payload.idempotencyKey())) {
            log.info(
                    "duplicate posting.transaction.received; idempotencyKey={} eventId={} postingId={} — skipped",
                    payload.idempotencyKey(), envelope.eventId(), payload.postingId());
            return;
        }

        log.info(
                "posting.transaction.received eventId={} postingId={} transactionRef={} correlationId={} idempotencyKey={} currency={} receivedAt={} legs={} metadata={}",
                envelope.eventId(),
                payload.postingId(),
                payload.transactionRef(),
                payload.correlationId(),
                payload.idempotencyKey(),
                payload.currency(),
                payload.receivedAt(),
                payload.legs(),
                payload.metadata()
        );
    }
}
