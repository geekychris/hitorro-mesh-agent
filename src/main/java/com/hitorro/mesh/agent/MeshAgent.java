/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.mesh.agent;

import com.hitorro.mesh.AgentDescriptor;
import com.hitorro.mesh.CancelMessage;
import com.hitorro.mesh.Codecs;
import com.hitorro.mesh.MeshTransport;
import com.hitorro.mesh.Subjects;
import com.hitorro.mesh.TaskDescriptor;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Wires a {@link MeshTransport} to a {@link TaskExecutor} + {@link HeartbeatPublisher}
 * for one agent identity. Callers construct it with whatever transport they have
 * (in-memory for demos, NATS in production), start it, and stop it via {@link #close()}.
 *
 * <p>The agent subscribes to exactly one control subject: {@code mesh.agent.task.<agentId>}.
 * The driver is responsible for capability-matching before it publishes there —
 * an agent that receives a task can assume the driver already validated it can
 * run it. If it can't, {@link TaskExecutor} returns an ERROR result.</p>
 */
public final class MeshAgent implements AutoCloseable {

    private final MeshTransport transport;
    private final AgentConfig config;
    private final RuntimeTableRegistry runtimeTables = new RuntimeTableRegistry();
    private final AtomicLong activeTasks = new AtomicLong();
    /** Phase 7m — lifetime counters exposed via {@link #tasksRunCount()},
     *  {@link #rowsEmittedCount()}, {@link #tasksErroredCount()},
     *  {@link #watermarksEmittedCount()}. TaskExecutor bumps them on task
     *  lifecycle events; app-side Micrometer wires them into Prometheus. */
    private final AtomicLong tasksRun = new AtomicLong();
    private final AtomicLong rowsEmitted = new AtomicLong();
    private final AtomicLong tasksErrored = new AtomicLong();
    private final AtomicLong watermarksEmitted = new AtomicLong();
    private final HeartbeatPublisher heartbeat;
    private final TaskExecutor executor;
    private MeshTransport.Subscription taskSub;
    private MeshTransport.Subscription controlSub;

    public MeshAgent(MeshTransport transport, AgentConfig config) {
        this.transport = transport;
        this.config = config;
        AgentDescriptor desc = new AgentDescriptor(
                config.agentId(), config.capabilities(), System.currentTimeMillis());
        this.heartbeat = new HeartbeatPublisher(
                transport, desc, config.heartbeatInterval().toMillis(), activeTasks);
        this.executor = new TaskExecutor(transport, config, runtimeTables, activeTasks,
                tasksRun, rowsEmitted, tasksErrored, watermarksEmitted);
    }

    public void start() {
        taskSub = transport.subscribe(Subjects.task(config.agentId()), bytes -> {
            TaskDescriptor td = Codecs.decode(bytes, TaskDescriptor.class);
            executor.submit(td);
        });
        // Phase 6c.2: subscribe to the control-subject wildcard so the driver
        // can cancel any in-flight query. Wildcard so we get every query's
        // control channel without having to re-subscribe per task.
        controlSub = transport.subscribe(Subjects.CONTROL_PREFIX + ">", bytes -> {
            CancelMessage cm = Codecs.decode(bytes, CancelMessage.class);
            executor.cancelQuery(cm.queryId());
        });
        heartbeat.start();
    }

    public String agentId() { return config.agentId(); }
    /** Exposed so app-side components (inventory responder etc.) can
     *  read boot-time table sets without re-plumbing AgentConfig. */
    public AgentConfig config() { return config; }

    /**
     * Registry of tables added at runtime via
     * {@link com.hitorro.mesh.RegisterTableMessage}. The app-side owns
     * the control-subject subscription (because building a concrete
     * {@code LocalTable} needs {@code NdjsonLocalTable} /
     * {@code ParquetLocalTable} which live in {@code hitorro-mesh-agent-app}).
     * MeshAgent just holds the registry so {@link TaskExecutor}'s lookup
     * can see it.
     */
    public RuntimeTableRegistry runtimeTables() { return runtimeTables; }

    /** Exposed so the app-side handler can subscribe to
     *  {@code mesh.agent.control.>} without re-plumbing the transport. */
    public MeshTransport transport() { return transport; }

    /** Currently in-flight task count. Phase 7m. */
    public long activeTaskCount() { return activeTasks.get(); }
    /** Cumulative tasks accepted (submitted to the worker pool) since start. */
    public long tasksRunCount() { return tasksRun.get(); }
    /** Cumulative result rows this agent has published (across all tasks). */
    public long rowsEmittedCount() { return rowsEmitted.get(); }
    /** Cumulative tasks that finished by publishing an ERROR ResultMessage. */
    public long tasksErroredCount() { return tasksErrored.get(); }
    /** Cumulative WATERMARK heartbeats published by streaming scan tasks. */
    public long watermarksEmittedCount() { return watermarksEmitted.get(); }

    @Override
    public void close() {
        if (taskSub != null) taskSub.close();
        if (controlSub != null) controlSub.close();
        heartbeat.close();
        executor.close();
    }
}
