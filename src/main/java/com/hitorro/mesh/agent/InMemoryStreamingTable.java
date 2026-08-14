/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.mesh.agent;

import com.fasterxml.jackson.databind.node.NullNode;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.jsontypesystem.Type;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A {@link LocalTable} whose {@link #openScan()} iterator blocks until new
 * rows are {@link #pushRow pushed} or the source is {@link #stop() stopped}.
 *
 * <p>Phase 6a: the foundation primitive for streaming queries. An agent that
 * hosts one of these behaves the same as with any other LocalTable — the
 * existing scan path in {@code TaskExecutor.runScanPlain} pulls from the
 * iterator, applies the pushed-down filter/project via jvssql, and publishes
 * rows to the result subject. The only difference is that {@code hasNext()}
 * blocks instead of returning false at end-of-list; queries stay alive as
 * long as the stream stays alive.</p>
 *
 * <h3>Termination</h3>
 *
 * <p>Call {@link #stop()} to inject a poison pill — the iterator returns
 * {@code false} on the next {@code hasNext()}, the agent's scan loop exits
 * cleanly, and the driver's {@code QueryHandle} sees EOS. Cluster shutdown
 * also unblocks the iterator via thread interruption (the agent's worker
 * pool uses {@code shutdownNow()}), so tests don't leak threads if they
 * forget to {@link #stop()}.</p>
 *
 * <h3>Real streaming sources (phase 6b+)</h3>
 *
 * <p>This class uses an in-memory {@link LinkedBlockingQueue} for test /
 * demo purposes. Production streaming will use dedicated {@code LocalTable}
 * implementations that read from {@code hitorro-streams-kafka} or
 * {@code hitorro-streams-nats} — same {@code LocalTable} interface, same
 * agent path, just a real source of unbounded rows. Interface is
 * deliberately unchanged so those bindings drop in without touching
 * mesh-agent.</p>
 */
public final class InMemoryStreamingTable implements LocalTable {

    /** Sentinel used to unblock the iterator on {@link #stop()}. */
    private static final JVS POISON = new JVS(NullNode.getInstance());

    private final String name;
    private final Type type;
    private final String partitionKey;
    private final LinkedBlockingQueue<JVS> queue = new LinkedBlockingQueue<>();

    public InMemoryStreamingTable(String name, Type type, String partitionKey) {
        this.name = name;
        this.type = type;
        this.partitionKey = partitionKey;
    }

    /**
     * Push a row into the stream. Immediately visible to any live scan
     * iterator's next {@code hasNext()} call.
     */
    public void pushRow(JVS row) {
        if (row == null) throw new NullPointerException("row");
        queue.add(row);
    }

    /**
     * Terminate the stream. Any live scan iterator returns {@code false}
     * from {@code hasNext()} once it reaches the poison pill; the agent's
     * scan loop publishes EOS and the driver's handle wraps up.
     */
    public void stop() {
        queue.add(POISON);
    }

    @Override public String name() { return name; }
    @Override public Type type() { return type; }
    @Override public String partitionKey() { return partitionKey; }

    @Override
    public Iterator<JVS> openScan() {
        return new Iterator<>() {
            private JVS peeked;
            private boolean done;

            @Override public boolean hasNext() {
                if (done) return false;
                if (peeked != null) return true;
                try {
                    JVS r = queue.take();       // blocks until row or poison
                    if (r == POISON) {
                        done = true;
                        return false;
                    }
                    peeked = r;
                    return true;
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    done = true;
                    return false;
                }
            }

            @Override public JVS next() {
                if (!hasNext()) throw new NoSuchElementException();
                JVS r = peeked;
                peeked = null;
                return r;
            }
        };
    }
}
