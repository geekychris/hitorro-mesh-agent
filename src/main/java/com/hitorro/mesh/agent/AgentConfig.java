/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.mesh.agent;

import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Agent runtime configuration. Populated by whoever launches the agent —
 * in Spring Boot from properties, in a plain launcher via a builder.
 * The {@code capabilities} list drives dispatch: the driver only sends
 * a task to an agent that advertises every {@code requiredCapabilities}.
 *
 * <p>Every agent should advertise the {@code jvssql} capability plus one
 * {@code partition:<table>:<key>} per local partition it holds.</p>
 */
public final class AgentConfig {
    private final String agentId;
    private final Set<String> capabilities;
    private final Duration heartbeatInterval;
    private final List<LocalTable> localTables;
    private final List<LocalTable> broadcastTables;

    public AgentConfig(String agentId,
                       Set<String> capabilities,
                       Duration heartbeatInterval,
                       List<LocalTable> localTables) {
        this(agentId, capabilities, heartbeatInterval, localTables, List.of());
    }

    /**
     * @param broadcastTables tables replicated to every agent (small dimension
     *                        tables); the executor auto-registers them in each
     *                        jvssql engine so queries can join to them. Phase 4a.
     */
    public AgentConfig(String agentId,
                       Set<String> capabilities,
                       Duration heartbeatInterval,
                       List<LocalTable> localTables,
                       List<LocalTable> broadcastTables) {
        this.agentId = agentId;
        this.capabilities = new LinkedHashSet<>(capabilities);
        this.heartbeatInterval = heartbeatInterval;
        this.localTables = List.copyOf(localTables);
        this.broadcastTables = List.copyOf(broadcastTables);
    }

    public String agentId() { return agentId; }
    public Set<String> capabilities() { return capabilities; }
    public Duration heartbeatInterval() { return heartbeatInterval; }
    public List<LocalTable> localTables() { return localTables; }
    public List<LocalTable> broadcastTables() { return broadcastTables; }
}
