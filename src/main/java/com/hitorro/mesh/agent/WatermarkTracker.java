/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.mesh.agent;

import com.fasterxml.jackson.databind.JsonNode;
import com.hitorro.jsontypesystem.JVS;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Phase 6d.2.1 — observes {@code event_time} on rows flowing through a
 * scan iterator and exposes the running max as a "watermark" — the
 * timestamp beyond which no more events are expected.
 *
 * <p>Used by the agent to publish WATERMARK heartbeats on streaming
 * scan tasks: a background thread reads {@link #current()} periodically
 * and emits {@code ResultMessage.WATERMARK} to the driver. Idle
 * partitions still emit heartbeats (from before the source went quiet),
 * unblocking the multi-partition combine's window closure.</p>
 *
 * <p>MVP: single-level event-time fields only (no dotted paths). The
 * field name is looked up directly on the row's top-level object.
 * If the field is missing or non-numeric on a row, that row is ignored
 * for watermark purposes.</p>
 *
 * <p>Sentinel {@link Long#MIN_VALUE} means "no watermark yet" — the
 * caller should skip publishing until at least one row has been observed.</p>
 */
final class WatermarkTracker {

    private final String eventTimeField;
    private final AtomicLong max = new AtomicLong(Long.MIN_VALUE);

    WatermarkTracker(String eventTimeField) {
        this.eventTimeField = eventTimeField;
    }

    /** Highest observed event-time, or {@link Long#MIN_VALUE} if no rows yet. */
    long current() {
        return max.get();
    }

    /** Wrap the upstream iterator so every row observed advances the tracker. */
    Iterator<JVS> wrap(Iterator<JVS> upstream) {
        return new Iterator<>() {
            @Override public boolean hasNext() { return upstream.hasNext(); }

            @Override public JVS next() {
                JVS jvs = upstream.next();
                observe(jvs);
                return jvs;
            }
        };
    }

    private void observe(JVS jvs) {
        if (jvs == null || eventTimeField == null) return;
        JsonNode node = jvs.getJsonNode();
        if (node == null) return;
        JsonNode f = node.get(eventTimeField);
        if (f == null || !f.isNumber()) return;
        long v = f.asLong();
        // Monotonic update — CAS loop keeps the highest value.
        while (true) {
            long cur = max.get();
            if (v <= cur) return;
            if (max.compareAndSet(cur, v)) return;
        }
    }
}
