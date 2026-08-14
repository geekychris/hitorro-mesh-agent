/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.mesh.agent;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hitorro.jsontypesystem.JVS;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the phase 6d.2.1 / 6d.2.2 watermark tracker. Verifies:
 * <ul>
 *   <li>Basic event-time observation via the wrapped iterator</li>
 *   <li>Monotonic max (out-of-order rows don't lower the watermark)</li>
 *   <li>Idle-timeout advancement — both the never-observed case and the
 *       "observed then went quiet" case</li>
 *   <li>Idle-timeout disabled by default (large timeout) leaves
 *       {@code currentWithIdle()} equivalent to {@code current()}</li>
 * </ul>
 */
class WatermarkTrackerTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static JVS row(long eventTime) throws Exception {
        return new JVS(MAPPER.readTree("{\"event_time\":" + eventTime + "}"));
    }

    @Test
    void newTracker_hasMinValueWatermark() {
        WatermarkTracker t = new WatermarkTracker("event_time");
        assertThat(t.current()).isEqualTo(Long.MIN_VALUE);
    }

    @Test
    void observingRow_advancesWatermarkToEventTime() throws Exception {
        WatermarkTracker t = new WatermarkTracker("event_time");
        Iterator<JVS> src = t.wrap(List.of(row(1000L)).iterator());
        while (src.hasNext()) src.next();
        assertThat(t.current()).isEqualTo(1000L);
    }

    @Test
    void observingMultipleRows_keepsHighestEventTime() throws Exception {
        WatermarkTracker t = new WatermarkTracker("event_time");
        Iterator<JVS> src = t.wrap(List.of(row(100L), row(500L), row(200L), row(400L)).iterator());
        while (src.hasNext()) src.next();
        assertThat(t.current()).as("out-of-order rows must not lower the watermark").isEqualTo(500L);
    }

    @Test
    void currentWithIdle_defaultTimeout_matchesCurrent() throws Exception {
        // Default constructor disables idle timeout (Long.MAX_VALUE) — no
        // matter how long we wait, idle logic shouldn't kick in.
        WatermarkTracker t = new WatermarkTracker("event_time");
        Iterator<JVS> src = t.wrap(List.of(row(1000L)).iterator());
        while (src.hasNext()) src.next();
        Thread.sleep(50);
        assertThat(t.currentWithIdle()).isEqualTo(t.current());
    }

    @Test
    void currentWithIdle_neverObserved_returnsMinValueBeforeTimeout() throws Exception {
        WatermarkTracker t = new WatermarkTracker("event_time", 1000L);
        assertThat(t.currentWithIdle()).isEqualTo(Long.MIN_VALUE);
    }

    @Test
    void currentWithIdle_neverObserved_advancesBySystemTimeAfterTimeout() throws Exception {
        // Very short timeout so the test completes fast. After the timeout
        // elapses, the tracker should start reporting a system-time-based
        // watermark even without any observed events — this is what
        // unblocks truly-idle partitions in the multi-partition combine.
        WatermarkTracker t = new WatermarkTracker("event_time", 50L);
        Thread.sleep(100);
        long wm = t.currentWithIdle();
        long now = System.currentTimeMillis();
        assertThat(wm).as("watermark should be system_now - timeout, roughly").isGreaterThan(Long.MIN_VALUE);
        assertThat(now - wm).as("watermark should be within a small delta of (now - timeout)").isBetween(0L, 200L);
    }

    @Test
    void currentWithIdle_observedThenIdle_advancesBeyondObservedMax() throws Exception {
        // Observe an event at t=1000, then wait past the idle timeout. The
        // returned watermark should be greater than 1000 by roughly the
        // excess idle time (elapsed - timeout).
        WatermarkTracker t = new WatermarkTracker("event_time", 50L);
        Iterator<JVS> src = t.wrap(List.of(row(1000L)).iterator());
        while (src.hasNext()) src.next();
        assertThat(t.currentWithIdle()).isEqualTo(1000L);   // fresh: no idle advance yet
        Thread.sleep(200);   // way past the 50ms timeout
        long wm = t.currentWithIdle();
        assertThat(wm).as("wm should advance past observed max after idle timeout").isGreaterThan(1000L);
        // Excess idle = elapsed - timeout ≈ 150ms. Wm should be 1000 + ~150.
        assertThat(wm).isBetween(1050L, 1500L);
    }

    @Test
    void currentWithIdle_recentObservation_returnsObservedMax() throws Exception {
        // If we observe an event AFTER the timeout window resets, wm
        // should snap back to observed max (no idle advancement).
        WatermarkTracker t = new WatermarkTracker("event_time", 50L);
        Iterator<JVS> src = t.wrap(List.of(row(1000L)).iterator());
        while (src.hasNext()) src.next();
        Thread.sleep(100);
        // Observation resets the last-observation timestamp
        Iterator<JVS> src2 = t.wrap(List.of(row(2000L)).iterator());
        while (src2.hasNext()) src2.next();
        // Immediately query — should be exactly 2000, no idle advance yet.
        assertThat(t.currentWithIdle()).isEqualTo(2000L);
    }

    @Test
    void observe_skipsRowsWithoutEventTimeField() throws Exception {
        // A row missing the event-time field is silently ignored — no
        // exception, watermark stays at prior value.
        WatermarkTracker t = new WatermarkTracker("event_time");
        Iterator<JVS> src = t.wrap(List.of(
                row(500L),
                new JVS(MAPPER.readTree("{\"other_field\":\"x\"}")),
                row(1000L)
        ).iterator());
        while (src.hasNext()) src.next();
        assertThat(t.current()).isEqualTo(1000L);
    }
}
