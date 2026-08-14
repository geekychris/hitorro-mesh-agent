/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.mesh.agent;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.jsontypesystem.Type;
import com.hitorro.jvssql.JvsSqlEngine;
import com.hitorro.jvssql.PreparedQuery;
import com.hitorro.mesh.Codecs;
import com.hitorro.mesh.CombineSpec;
import com.hitorro.mesh.MeshTransport;
import com.hitorro.mesh.ResultMessage;
import com.hitorro.mesh.ShuffleSpec;
import com.hitorro.mesh.TaskDescriptor;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Runs a {@link TaskDescriptor}. Three shapes (chosen by task.stage +
 * task.shuffleSpec + task.combineSpec):
 *
 * <ol>
 *   <li><b>stage-0 plain</b> — run SQL over local partition, publish rows
 *       to {@code resultSubject}. Phase-1/2 behavior.</li>
 *   <li><b>stage-0 shuffle sink</b> — run SQL over local partition, hash
 *       each output row into one of {@code shuffleSpec.buckets} shuffle
 *       bucket subjects. Publish EOS to every bucket at end.</li>
 *   <li><b>stage-1 combine worker</b> — subscribe to a shuffle bucket,
 *       buffer rows, count EOS-by-partitionKey until all upstream
 *       partitions have EOS'd, then run the combine SQL over the buffer
 *       and publish output rows + EOS to {@code resultSubject}.</li>
 * </ol>
 *
 * <p><b>Concurrency:</b> one worker thread per active task. Combine
 * workers block for the entire duration of the query (waiting for
 * shuffle input) — this is why the worker pool is generously sized
 * (2× CPU count minimum).</p>
 *
 * <p><b>Correctness of combine:</b> we synthesize the combine's input
 * {@link Type} by inspecting the FIRST row's columns and defaulting all
 * of them to {@code core_string} or {@code core_long} based on JsonNode
 * type. Good enough for the MVP shape (group cols + numeric aggregates);
 * a phase-3 improvement is to carry the schema in {@link CombineSpec}.</p>
 */
final class TaskExecutor implements AutoCloseable {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final MeshTransport transport;
    private final AgentConfig config;
    private final AtomicLong activeTasks;
    private final java.util.concurrent.ExecutorService workers;
    /**
     * queryId → Futures for every task running under that query. Populated
     * on submit, drained when a task completes. Consulted from the control
     * subscription to cancel all workers for a given query.
     *
     * <p>ConcurrentHashMap of CopyOnWriteArrayList lets the control thread
     * iterate + interrupt safely while worker threads add/remove entries.</p>
     */
    private final java.util.concurrent.ConcurrentHashMap<String, java.util.List<java.util.concurrent.Future<?>>>
            activeByQuery = new java.util.concurrent.ConcurrentHashMap<>();

    TaskExecutor(MeshTransport transport, AgentConfig config, AtomicLong activeTasks) {
        this.transport = transport;
        this.config = config;
        this.activeTasks = activeTasks;
        this.workers = Executors.newFixedThreadPool(
                Math.max(4, Runtime.getRuntime().availableProcessors() * 2),
                r -> {
                    Thread t = new Thread(r, "mesh-agent-worker-" + config.agentId());
                    t.setDaemon(true);
                    return t;
                });
    }

    void submit(TaskDescriptor task) {
        java.util.concurrent.Future<?> f = workers.submit(() -> runOne(task));
        activeByQuery.computeIfAbsent(task.queryId(),
                k -> new java.util.concurrent.CopyOnWriteArrayList<>()).add(f);
    }

    /**
     * Cancel every in-flight task for a given query. Called from the mesh
     * agent's control-subject subscription when the driver publishes a
     * {@link com.hitorro.mesh.CancelMessage}. Interrupting the worker
     * threads unblocks streaming iterators (they respond to
     * {@link InterruptedException} and terminate), the scan loop exits,
     * and the agent publishes EOS to the result subject as usual — so
     * downstream cleanup follows the same path as a naturally-terminating
     * query.
     */
    void cancelQuery(String queryId) {
        java.util.List<java.util.concurrent.Future<?>> futures = activeByQuery.remove(queryId);
        if (futures == null) return;
        for (var f : futures) f.cancel(true);
    }

    private void runOne(TaskDescriptor task) {
        activeTasks.incrementAndGet();
        try {
            if (task.combineSpec() != null) {
                runCombine(task);
            } else if (task.shuffleSpec() != null) {
                runScanShuffled(task);
            } else {
                runScanPlain(task);
            }
        } catch (java.util.concurrent.CancellationException ce) {
            // Task was cancelled via cancelQuery(). Silent — the driver already
            // knows (it sent the cancel). No error message needed on the wire.
        } catch (Throwable t) {
            publishError(task, "task " + task.taskId() + " failed: "
                    + t.getClass().getSimpleName() + ": " + t.getMessage());
        } finally {
            activeTasks.decrementAndGet();
            // Drain the Future entry from the active-by-query map. If cancelQuery
            // already ran (interrupted us mid-task), it already removed the whole
            // list, so this is a no-op. Preventing unbounded growth is the point.
            var list = activeByQuery.get(task.queryId());
            if (list != null) {
                list.removeIf(java.util.concurrent.Future::isDone);
                if (list.isEmpty()) activeByQuery.remove(task.queryId(), list);
            }
        }
    }

    // -- shape 1: plain stage-0 scan ------------------------------------------

    private void runScanPlain(TaskDescriptor task) {
        LocalTable local = requireLocalTable(task);
        JvsSqlEngine engine = engineWithBroadcasts()
                .registerStream(task.sourceTable(), local.openScan(), local.type())
                .build();
        PreparedQuery q = engine.compile(task.sqlPlan());
        long seq = 0;
        Iterator<JsonNode> rows = q.asIterator();
        while (rows.hasNext()) {
            JsonNode row = rows.next();
            transport.publish(task.resultSubject(),
                    Codecs.encode(ResultMessage.row(task.taskId(), task.partitionKey(), row, seq++)));
        }
        transport.publish(task.resultSubject(),
                Codecs.encode(ResultMessage.eos(task.taskId(), task.partitionKey(), seq)));
    }

    // -- shape 2: stage-0 scan with shuffle sink ------------------------------

    private void runScanShuffled(TaskDescriptor task) {
        LocalTable local = requireLocalTable(task);
        ShuffleSpec spec = task.shuffleSpec();
        List<String> keyCols = spec.keyColumns();
        int buckets = spec.buckets();

        JvsSqlEngine engine = engineWithBroadcasts()
                .registerStream(task.sourceTable(), local.openScan(), local.type())
                .build();
        PreparedQuery q = engine.compile(task.sqlPlan());

        // Per-bucket sequence counters — useful for debugging via NATS logs.
        long[] seq = new long[buckets];
        Iterator<JsonNode> rows = q.asIterator();
        while (rows.hasNext()) {
            JsonNode row = rows.next();
            int b = bucketFor(row, keyCols, buckets);
            transport.publish(spec.subjectFor(b),
                    Codecs.encode(ResultMessage.row(task.taskId(), task.partitionKey(), row, seq[b]++)));
        }
        // Signal end-of-stream to every bucket — combine workers count these to know
        // when their bucket is complete. Every stage-0 partition sends one EOS per bucket.
        for (int b = 0; b < buckets; b++) {
            transport.publish(spec.subjectFor(b),
                    Codecs.encode(ResultMessage.eos(task.taskId(), task.partitionKey(), seq[b])));
        }
    }

    // -- shape 3: stage-1 combine worker --------------------------------------

    private void runCombine(TaskDescriptor task) throws Exception {
        CombineSpec spec = task.combineSpec();
        // Subscribe to every input's bucket subject, sharing one inbox. Each
        // message carries which input it came from via a small envelope.
        LinkedBlockingQueue<Envelope> inbox = new LinkedBlockingQueue<>();
        java.util.List<MeshTransport.Subscription> subs = new java.util.ArrayList<>();
        for (int i = 0; i < spec.inputs().size(); i++) {
            final int idx = i;
            var input = spec.inputs().get(idx);
            subs.add(transport.subscribe(input.shuffleInputSubject(), bytes -> {
                ResultMessage m = Codecs.decode(bytes, ResultMessage.class);
                inbox.add(new Envelope(idx, m));
            }));
        }
        try {
            // One buffer + one upstream-done set per input.
            int nInputs = spec.inputs().size();
            @SuppressWarnings("unchecked")
            java.util.List<JsonNode>[] buffers = new java.util.List[nInputs];
            Set<String>[] upstreamDone = new Set[nInputs];
            for (int i = 0; i < nInputs; i++) {
                buffers[i] = new java.util.ArrayList<>();
                upstreamDone[i] = new HashSet<>();
            }

            long deadline = System.currentTimeMillis() + 30_000;
            while (!allInputsComplete(spec, upstreamDone)) {
                Envelope env = inbox.poll(Math.max(50, deadline - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
                if (env == null) {
                    throw new RuntimeException("combine worker timed out waiting for shuffle input; state: "
                            + progressReport(spec, upstreamDone, buffers));
                }
                switch (env.msg.kind()) {
                    case ROW   -> buffers[env.inputIdx].add(env.msg.row());
                    case EOS   -> upstreamDone[env.inputIdx].add(env.msg.partitionKey());
                    case ERROR -> throw new RuntimeException("upstream error on input " + env.inputIdx
                            + ": " + env.msg.errorMessage());
                }
            }

            // Phase 4c.1 short-circuit — the rules for whether an empty
            // input trivializes the combine depend on join semantics:
            //   INNER / NONE (aggregate): any-empty → result is empty.
            //   LEFT:  only if left is empty (right-empty must still null-pad left).
            //   RIGHT: only if right is empty.
            //   FULL:  only if BOTH are empty.
            // For the non-short-circuited OUTER cases we need the empty side
            // registered with its real schema so jvssql can null-pad it —
            // that's what the InputSource.schemaTypeJson field carries.
            if (canShortCircuit(spec, buffers)) {
                transport.publish(task.resultSubject(),
                        Codecs.encode(ResultMessage.eos(task.taskId(), task.taskId(), 0)));
                return;
            }

            // Build the combine engine: broadcast tables (if any) + one
            // registered stream per input. Prefer the schema shipped from
            // the driver (typed even when the buffer is empty). Fall back
            // to first-row synthesis for the phase-2.5.1 aggregate-combine
            // path that doesn't ship a schema.
            JvsSqlEngine.Builder builder = engineWithBroadcasts();
            for (int i = 0; i < nInputs; i++) {
                var input = spec.inputs().get(i);
                Type t = input.schemaTypeJson() != null
                        ? typeFromJson(input.schemaTypeJson())
                        : synthesizeType(input.combineInputTable(), buffers[i]);
                builder.registerStream(input.combineInputTable(), jvsIterator(buffers[i]), t);
            }
            JvsSqlEngine engine = builder.build();
            PreparedQuery q = engine.compile(task.sqlPlan());

            long seq = 0;
            Iterator<JsonNode> out = q.asIterator();
            while (out.hasNext()) {
                JsonNode row = out.next();
                transport.publish(task.resultSubject(),
                        Codecs.encode(ResultMessage.row(task.taskId(),
                                task.taskId(),
                                row, seq++)));
            }
            transport.publish(task.resultSubject(),
                    Codecs.encode(ResultMessage.eos(task.taskId(), task.taskId(), seq)));
        } finally {
            for (var s : subs) s.close();
        }
    }

    /**
     * Whether an empty input renders the whole combine trivially empty.
     * See phase 4c.1 rules in the caller's comment. Two-input rules assume
     * the InputSource list order is {@code [left, right]} — the shuffle-join
     * dispatcher builds them that way explicitly.
     */
    private static boolean canShortCircuit(CombineSpec spec, java.util.List<JsonNode>[] buffers) {
        int n = buffers.length;
        CombineSpec.JoinKind kind = spec.joinKind() == null ? CombineSpec.JoinKind.NONE : spec.joinKind();
        if (n == 2) {
            boolean leftEmpty = buffers[0].isEmpty();
            boolean rightEmpty = buffers[1].isEmpty();
            return switch (kind) {
                case LEFT -> leftEmpty;
                case RIGHT -> rightEmpty;
                case FULL -> leftEmpty && rightEmpty;
                case INNER, NONE -> leftEmpty || rightEmpty;
            };
        }
        // Single-input or aggregate-combine — any-empty short-circuit is safe.
        for (var b : buffers) if (b.isEmpty()) return true;
        return false;
    }

    private static Type typeFromJson(String json) throws Exception {
        Type t = new Type();
        t.init(MAPPER.readTree(json));
        return t;
    }

    private static boolean allInputsComplete(CombineSpec spec, Set<String>[] done) {
        for (int i = 0; i < spec.inputs().size(); i++) {
            if (done[i].size() < spec.inputs().get(i).expectedUpstreamPartitions()) return false;
        }
        return true;
    }

    private static String progressReport(CombineSpec spec, Set<String>[] done, java.util.List<JsonNode>[] bufs) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < spec.inputs().size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append("input[").append(i).append(":").append(spec.inputs().get(i).combineInputTable()).append("] ")
              .append(done[i].size()).append("/").append(spec.inputs().get(i).expectedUpstreamPartitions())
              .append(" upstreams done, ").append(bufs[i].size()).append(" rows buffered");
        }
        return sb.toString();
    }

    private record Envelope(int inputIdx, ResultMessage msg) {}

    // -- helpers --------------------------------------------------------------

    /**
     * Bucket assignment — same math as {@code ShufflePlacement.bucketFor}
     * in the driver module. Copied rather than shared to avoid a
     * mesh-agent → mesh-driver dependency (agents can't depend on driver code).
     * A future refactor moves it into mesh-core.
     */
    static int bucketFor(JsonNode row, List<String> keyColumns, int buckets) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < keyColumns.size(); i++) {
            if (i > 0) sb.append('\0');
            JsonNode v = row == null ? null : row.get(keyColumns.get(i));
            if (v == null || v.isNull()) sb.append("\0__NULL__\0");
            else sb.append(v.asText());
        }
        return Math.floorMod(sb.toString().hashCode(), buckets);
    }

    /**
     * Best-effort type synthesis by inspecting the first row. Sees any column
     * whose value is numeric across the sample as {@code core_long}, everything
     * else as {@code core_string}. Good enough for combine SQL that operates
     * on group cols (strings) + aggregate cols (longs).
     */
    private static Type synthesizeType(String tableName, java.util.List<JsonNode> sample) throws Exception {
        StringBuilder json = new StringBuilder();
        json.append("{\"name\":\"").append(tableName).append("\",\"fields\":[");
        if (sample.isEmpty()) {
            // No rows arrived — combine will return no rows regardless. Emit an empty type.
            json.append("]}");
        } else {
            JsonNode first = sample.get(0);
            boolean firstField = true;
            for (Iterator<String> it = first.fieldNames(); it.hasNext(); ) {
                String name = it.next();
                if (!firstField) json.append(",");
                firstField = false;
                boolean numeric = first.get(name).isNumber();
                json.append("{\"name\":\"").append(name)
                    .append("\",\"type\":\"").append(numeric ? "core_long" : "core_string")
                    .append("\"}");
            }
            json.append("]}");
        }
        Type t = new Type();
        t.init(MAPPER.readTree(json.toString()));
        return t;
    }

    private static Iterator<JVS> jvsIterator(java.util.List<JsonNode> rows) {
        return new Iterator<>() {
            int i = 0;
            @Override public boolean hasNext() { return i < rows.size(); }
            @Override public JVS next() { return new JVS(rows.get(i++)); }
        };
    }

    /**
     * Fresh {@code JvsSqlEngine} builder with every broadcast table registered.
     * Callers then chain {@code .registerStream(sourceTable, ...)} for whatever
     * partition-local (or shuffle-input) stream this task needs.
     *
     * <p>Registering broadcast tables on every engine is deliberately dumb:
     * the alternative (parse the task's SQL, figure out which broadcast tables
     * are actually referenced, register only those) doesn't pay for itself
     * when broadcast tables are tiny — the cost is a few extra Calcite schema
     * entries per query. If a broadcast table's data is large we'd need a
     * cheaper strategy (lazy registration, or a shared engine cache).</p>
     */
    private JvsSqlEngine.Builder engineWithBroadcasts() {
        JvsSqlEngine.Builder b = JvsSqlEngine.builder();
        for (LocalTable bt : config.broadcastTables()) {
            b.registerStream(bt.name(), bt.openScan(), bt.type());
        }
        return b;
    }

    private LocalTable requireLocalTable(TaskDescriptor task) {
        for (LocalTable lt : config.localTables()) {
            if (!lt.name().equals(task.sourceTable())) continue;
            String pk = lt.partitionKey();
            if (pk == null ? task.partitionKey() == null : pk.equals(task.partitionKey())) return lt;
        }
        throw new IllegalStateException("agent " + config.agentId()
                + " does not hold partition " + task.partitionKey()
                + " of table " + task.sourceTable());
    }

    private void publishError(TaskDescriptor task, String msg) {
        transport.publish(task.resultSubject(),
                Codecs.encode(ResultMessage.error(task.taskId(), task.partitionKey(), msg)));
    }

    @Override
    public void close() {
        workers.shutdownNow();
    }
}
