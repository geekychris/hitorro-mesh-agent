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

/**
 * Fires a heartbeat message on a fixed cadence. Simple by design — no
 * jitter, no failure detection, no backoff. The driver's registry decides
 * how many missed intervals count as dead.
 */
final class HeartbeatPublisher implements AutoCloseable {

    private final MeshTransport transport;
    private final AgentDescriptor descriptor;
    private final long intervalMillis;
    private final AtomicLong activeTasks;
    private final ScheduledExecutorService scheduler;

    HeartbeatPublisher(MeshTransport transport,
                       AgentDescriptor descriptor,
                       long intervalMillis,
                       AtomicLong activeTasks) {
        this.transport = transport;
        this.descriptor = descriptor;
        this.intervalMillis = intervalMillis;
        this.activeTasks = activeTasks;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "mesh-heartbeat-" + descriptor.agentId());
            t.setDaemon(true);
            return t;
        });
    }

    void start() {
        scheduler.scheduleAtFixedRate(this::publishOne, 0, intervalMillis, TimeUnit.MILLISECONDS);
    }

    private void publishOne() {
        try {
            HeartbeatMessage msg = new HeartbeatMessage(
                    descriptor,
                    System.currentTimeMillis(),
                    activeTasks.get());
            transport.publish(Subjects.heartbeat(descriptor.agentId()), Codecs.encode(msg));
        } catch (Throwable ignore) {
            // heartbeat is best-effort; a hiccup here shouldn't kill the agent
        }
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }
}
