package com.corebanking.ledger.dedup;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * In-memory idempotency-key dedup. Bounded LRU set; oldest keys evict once the
 * configured capacity is reached. Not durable across restarts and not shared
 * across replicas — good enough for the log-only ledger, but every real
 * consumer of posting.transaction.received will need a durable dedup store
 * (Postgres row on the money-mutating write, Redis SET NX, etc.) before it
 * ships side effects.
 */
@Component
public class ProcessedEventTracker {

    private final int capacity;
    private final Map<String, Boolean> seen;

    public ProcessedEventTracker(@Value("${ledger.dedup.capacity:100000}") int capacity) {
        this.capacity = capacity;
        LinkedHashMap<String, Boolean> map = new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                return size() > ProcessedEventTracker.this.capacity;
            }
        };
        this.seen = Collections.synchronizedMap(map);
    }

    /**
     * @return true if the key was newly added (caller should process); false if
     *         the key was already seen within the dedup window (caller should skip).
     */
    public boolean markSeen(String idempotencyKey) {
        synchronized (seen) {
            return seen.putIfAbsent(idempotencyKey, Boolean.TRUE) == null;
        }
    }
}
