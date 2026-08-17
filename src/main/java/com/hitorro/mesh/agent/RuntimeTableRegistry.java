/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.agent;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe registry of tables added at runtime by the driver via
 * {@link com.hitorro.mesh.RegisterTableMessage}. Sits alongside the
 * immutable {@link AgentConfig#localTables()} / {@link AgentConfig#broadcastTables()}
 * — {@link TaskExecutor} consults both, checking runtime first so a
 * runtime-registered table with the same name shadows any boot-time
 * entry (useful for developer iteration on the same table name).
 *
 * <p>Registration is idempotent by {@code (name, partitionKey)}: a
 * subsequent register for the same key replaces the old
 * {@link LocalTable}. Rows are already loaded eagerly inside
 * {@code NdjsonLocalTable} / {@code ParquetLocalTable} so replacement
 * is a fast pointer swap; the outgoing table's rows are GC'd once
 * in-flight scans finish (each scan copies rows into its own iterator).</p>
 *
 * <p>Broadcast entries carry {@code partitionKey == null}; distributed
 * entries carry a non-null key. Same as the boot-time convention.</p>
 */
public final class RuntimeTableRegistry {

    /** key: name (broadcast) OR name + "@" + partitionKey (distributed). */
    private final ConcurrentHashMap<String, LocalTable> tables = new ConcurrentHashMap<>();

    private static String key(String name, String partitionKey) {
        return partitionKey == null ? name : name + "@" + partitionKey;
    }

    /** Install (or replace) a table. Both broadcast and distributed. */
    public void register(LocalTable table) {
        tables.put(key(table.name(), table.partitionKey()), table);
    }

    /** Drop a table by name + partition key. No-op if absent. */
    public void unregister(String name, String partitionKey) {
        tables.remove(key(name, partitionKey));
    }

    /** Look up a specific partition. Returns null if not registered. */
    public LocalTable find(String name, String partitionKey) {
        return tables.get(key(name, partitionKey));
    }

    /** Snapshot of every runtime broadcast table (partitionKey == null). */
    public List<LocalTable> broadcastSnapshot() {
        List<LocalTable> out = new ArrayList<>();
        for (LocalTable t : tables.values()) {
            if (t.partitionKey() == null) out.add(t);
        }
        return out;
    }

    /** Snapshot of every runtime distributed-partition table. */
    public List<LocalTable> localSnapshot() {
        List<LocalTable> out = new ArrayList<>();
        for (LocalTable t : tables.values()) {
            if (t.partitionKey() != null) out.add(t);
        }
        return out;
    }

    /** Total table count, both kinds. */
    public int size() {
        return tables.size();
    }
}
