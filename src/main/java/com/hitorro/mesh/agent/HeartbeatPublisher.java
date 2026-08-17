/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.mesh.agent;

import com.hitorro.mesh.AgentDescriptor;
import com.hitorro.mesh.Codecs;
import com.hitorro.mesh.HeartbeatMessage;
import com.hitorro.mesh.MeshTransport;
import com.hitorro.mesh.Subjects;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Fires a heartbeat message on a fixed cadence. Simple by design — no
 * jitter, no failure detection, no backoff. The driver's registry decides
 * how many missed intervals count as dead.
 *
 * <p>Takes a {@link Supplier} of {@link AgentDescriptor} so each tick
 * reflects the CURRENT capability set — an agent that mutates its
 * runtime capabilities (e.g. after installing a partition via
 * {@code RegisterTableMessage}) sees the change advertised on the
 * next heartbeat without a restart.</p>
 */
final class HeartbeatPublisher implements AutoCloseable {

    private final MeshTransport transport;
    private final Supplier<AgentDescriptor> descriptorSupplier;
    private final long intervalMillis;
    private final AtomicLong activeTasks;
    private final ScheduledExecutorService scheduler;
    private final String agentId;

    HeartbeatPublisher(MeshTransport transport,
                       Supplier<AgentDescriptor> descriptorSupplier,
                       long intervalMillis,
                       AtomicLong activeTasks) {
        this.transport = transport;
        this.descriptorSupplier = descriptorSupplier;
        this.intervalMillis = intervalMillis;
        this.activeTasks = activeTasks;
        this.agentId = descriptorSupplier.get().agentId();
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "mesh-heartbeat-" + agentId);
            t.setDaemon(true);
            return t;
        });
    }

    /** Back-compat overload: fixed descriptor, no capability mutation. */
    HeartbeatPublisher(MeshTransport transport,
                       AgentDescriptor descriptor,
                       long intervalMillis,
                       AtomicLong activeTasks) {
        this(transport, () -> descriptor, intervalMillis, activeTasks);
    }

    void start() {
        scheduler.scheduleAtFixedRate(this::publishOne, 0, intervalMillis, TimeUnit.MILLISECONDS);
    }

    private void publishOne() {
        try {
            AgentDescriptor desc = descriptorSupplier.get();
            HeartbeatMessage msg = new HeartbeatMessage(
                    desc,
                    System.currentTimeMillis(),
                    activeTasks.get());
            transport.publish(Subjects.heartbeat(desc.agentId()), Codecs.encode(msg));
        } catch (Throwable ignore) {
            // heartbeat is best-effort; a hiccup here shouldn't kill the agent
        }
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }
}
