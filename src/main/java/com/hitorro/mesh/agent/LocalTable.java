/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.mesh.agent;

import com.hitorro.jsontypesystem.JVS;
import com.hitorro.jsontypesystem.Type;
import com.hitorro.jvssql.config.StreamConfig;

import java.util.Iterator;

/**
 * What the agent knows about a table it can serve. The agent doesn't run the
 * full jvssql planner over remote data — it opens a scan on this local
 * partition and lets jvssql filter/project it locally.
 *
 * <p>The agent binds a jvssql table under the same {@code sourceTable} name the
 * driver used in the task's SQL, then jvssql resolves the identifier
 * ({@code FROM docs}) to this local partition scan.</p>
 */
public interface LocalTable {

    /** Logical table name that appears in the driver's SQL FROM clause. */
    String name();

    /** JVS Type. Used by jvssql for schema validation on the agent side. */
    Type type();

    /** Which partition of this table this agent holds (or {@code null} for non-partitioned). */
    String partitionKey();

    /**
     * Open a fresh iterator over the local partition. Called once per assigned task —
     * the agent MUST return a new iterator each time; a task consumed once cannot be replayed.
     */
    Iterator<JVS> openScan();

    /**
     * Phase 6d.1 — jvssql {@link StreamConfig} for this table. Return
     * non-null to declare this table a streaming source; the agent will
     * register it with jvssql as {@code registerStream(name, iter, type, streamConfig)}
     * so windowed aggregates auto-swap to the incremental
     * {@code StreamingAggregate} executor and rows emit as watermarks
     * advance instead of at end-of-scan.
     *
     * <p>Default {@code null} → batch behaviour, unchanged from earlier
     * phases. Streaming {@code LocalTable} impls (e.g.
     * {@link InMemoryStreamingTable}, Kafka / NATS adapters) override
     * this to supply their event-time field.</p>
     */
    default StreamConfig streamConfig() { return null; }
}
