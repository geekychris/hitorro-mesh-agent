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
    private final AtomicLong activeTasks = new AtomicLong();
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
        this.executor = new TaskExecutor(transport, config, activeTasks);
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

    @Override
    public void close() {
        if (taskSub != null) taskSub.close();
        if (controlSub != null) controlSub.close();
        heartbeat.close();
        executor.close();
    }
}
