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
    private final long idleTimeoutMs;
    private final long createdSystemMs = System.currentTimeMillis();
    private final AtomicLong max = new AtomicLong(Long.MIN_VALUE);
    private volatile long lastObservationSystemMs = 0L;

    WatermarkTracker(String eventTimeField) {
        this(eventTimeField, Long.MAX_VALUE);   // idle-timeout disabled by default
    }

    WatermarkTracker(String eventTimeField, long idleTimeoutMs) {
        this.eventTimeField = eventTimeField;
        this.idleTimeoutMs = idleTimeoutMs;
    }

    /** Highest observed event-time, or {@link Long#MIN_VALUE} if no rows yet. */
    long current() {
        return max.get();
    }

    /**
     * Phase 6d.2.2 — watermark with idle-timeout advancement. If no events
     * have been observed within {@code idleTimeoutMs}, the watermark
     * advances based on wall-clock elapsed time (assuming event-time
     * flows roughly at real-time rate). This unblocks global window
     * closure for partitions that are quiet for extended periods.
     *
     * <p>Rules:</p>
     * <ul>
     *   <li>If we've observed at least one event AND idle > timeout →
     *       advance by (idleMs - timeout) beyond the observed max.</li>
     *   <li>If we've observed nothing AND time-since-creation > timeout →
     *       use {@code system_now - timeout} as the watermark.</li>
     *   <li>Otherwise → return {@link #current()}.</li>
     * </ul>
     *
     * <p><b>Caveat:</b> assumes event-time is roughly aligned with wall-clock
     * (real-time streams). For backfill / historical replay where event-time
     * is arbitrary, set {@code idleTimeoutMs} very large or leave at the
     * default {@link Long#MAX_VALUE} to disable this mechanism.</p>
     */
    long currentWithIdle() {
        long observed = max.get();
        long now = System.currentTimeMillis();
        if (observed == Long.MIN_VALUE) {
            long elapsed = now - createdSystemMs;
            return elapsed > idleTimeoutMs ? now - idleTimeoutMs : Long.MIN_VALUE;
        }
        long idle = now - lastObservationSystemMs;
        if (idle > idleTimeoutMs) {
            return observed + (idle - idleTimeoutMs);
        }
        return observed;
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
            if (v <= cur) break;
            if (max.compareAndSet(cur, v)) break;
        }
        lastObservationSystemMs = System.currentTimeMillis();
    }
}
