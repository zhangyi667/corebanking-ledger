package com.corebanking.ledger.events;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public record PostingTransactionReceived(
        UUID postingId,
        String transactionRef,
        String correlationId,
        String idempotencyKey,
        String currency,
        Instant receivedAt,
        List<Leg> legs,
        Map<String, Object> metadata
) {
    public record Leg(String accountId, LegType type, BigDecimal amount) {
    }

    public enum LegType {DEBIT, CREDIT}
}
