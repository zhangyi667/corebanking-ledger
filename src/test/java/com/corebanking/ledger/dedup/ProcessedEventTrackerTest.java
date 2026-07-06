package com.corebanking.ledger.dedup;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ProcessedEventTrackerTest {

    @Test
    void firstSightIsNewSubsequentIsDuplicate() {
        ProcessedEventTracker t = new ProcessedEventTracker(1000);
        assertThat(t.markSeen("k1")).isTrue();
        assertThat(t.markSeen("k1")).isFalse();
        assertThat(t.markSeen("k1")).isFalse();
    }

    @Test
    void distinctKeysAreIndependent() {
        ProcessedEventTracker t = new ProcessedEventTracker(1000);
        assertThat(t.markSeen("a")).isTrue();
        assertThat(t.markSeen("b")).isTrue();
        assertThat(t.markSeen("a")).isFalse();
        assertThat(t.markSeen("b")).isFalse();
    }

    @Test
    void evictedKeyReappearsAsNew() {
        ProcessedEventTracker t = new ProcessedEventTracker(2);
        t.markSeen("a");
        t.markSeen("b");
        // "a" is evicted when "c" pushes size past 2 (LRU order: a, b, c -> drops a).
        t.markSeen("c");
        assertThat(t.markSeen("a")).isTrue();
    }

    @Test
    void accessingAKeyRefreshesItsLruPosition() {
        ProcessedEventTracker t = new ProcessedEventTracker(2);
        t.markSeen("a");                                // LRU order: [a]
        t.markSeen("b");                                // LRU order: [a, b]
        assertThat(t.markSeen("a")).isFalse();          // touching "a" -> [b, a]
        t.markSeen("c");                                // insert c, evict eldest = "b"
        assertThat(t.markSeen("b")).isTrue();           // b was evicted
    }
}
