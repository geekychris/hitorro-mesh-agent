/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.mesh.agent;

import com.hitorro.jsontypesystem.JVS;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.lang.reflect.Method;
import java.util.*;

/**
 * Pins the agent's JSON-dependent decisions to 3.0.2 on Jackson 2: the shuffle bucket each row goes
 * to ({@link TaskExecutor#bucketFor}, which must agree with the driver's {@code ShufflePlacement}),
 * the event-time watermark for every kind of value, and the column types the combine step
 * synthesizes from a sample row. {@code jackson2-golden.txt} was produced by running the same
 * program against the 3.0.2 classes on Jackson 2.18.
 */
class Jackson2GoldenCompatTest {
  static final String[] ROWS = {
    "{\"k\":\"a\",\"n\":1,\"d\":2.5,\"b\":true}",
    "{\"k\":\"a\",\"n\":12345678901,\"d\":1.10,\"b\":false}",
    "{\"k\":\"\u00e9\u20ac\ud83d\ude00\",\"n\":12345678901234567890,\"d\":-0.0,\"b\":null}",
    "{\"k\":null,\"n\":\"7\",\"d\":1e30}",
    "{\"n\":-3,\"d\":3.7,\"b\":\"x\"}",
    "{\"k\":{\"x\":1},\"n\":[1,2],\"d\":{},\"b\":[]}",
    "{\"k\":\"\",\"n\":0,\"d\":0.0,\"b\":1}",
    "{\"k\":\"__NULL__\",\"n\":1.0,\"d\":1E+2,\"b\":\"\\u0000\"}"
  };
  static final List<List<String>> KEYS = List.of(List.of("k"), List.of("n"), List.of("d"), List.of("b"), List.of("k", "n"), List.of("n", "k", "d", "b"), List.of("missing"), List.of());
  static final int[] BUCKETS = {1, 4, 7, 16, 1000};
  static final String[] TIMES = {"5", "3", "3.7", "\"100\"", "null", "12345678901", "-1", "1e30", "12345678901234567890", "99999999999999999999.5", "true", "{}", "[7]", "1.10"};

  static String generate() throws Exception {
    tools.jackson.databind.ObjectMapper m = com.hitorro.util.json.JsonMappers.json();
    StringBuilder out = new StringBuilder();
    for (int r = 0; r < ROWS.length; r++) {
      tools.jackson.databind.JsonNode row = m.readTree(ROWS[r]);
      for (int k = 0; k < KEYS.size(); k++)
        for (int b : BUCKETS)
          out.append("bucket.row").append(r).append(".keys").append(k).append(".b").append(b).append('=')
             .append(TaskExecutor.bucketFor(row, KEYS.get(k), b)).append('\n');
    }
    out.append("bucket.nullrow=").append(TaskExecutor.bucketFor(null, List.of("k", "n"), 16)).append('\n');
    for (int i = 0; i < TIMES.length; i++) {
      WatermarkTracker w = new WatermarkTracker("ts");
      Iterator<JVS> it = w.wrap(List.of(new JVS(m.readTree("{\"ts\":" + TIMES[i] + "}"))).iterator());
      String v;
      try { it.next(); v = String.valueOf(w.current()); } catch (Throwable t) { v = "!" + t.getClass().getSimpleName(); }
      out.append("watermark.single.").append(i).append('=').append(v).append('\n');
    }
    WatermarkTracker w = new WatermarkTracker("ts");
    List<JVS> seq = new ArrayList<>();
    for (String t : TIMES) seq.add(new JVS(m.readTree("{\"ts\":" + t + "}")));
    Iterator<JVS> it = w.wrap(seq.iterator());
    StringBuilder sb = new StringBuilder();
    while (it.hasNext()) { it.next(); sb.append(w.current()).append(','); }
    out.append("watermark.sequence=").append(sb).append('\n');
    Method st = TaskExecutor.class.getDeclaredMethod("synthesizeType", String.class, List.class);
    st.setAccessible(true);
    for (int r = 0; r < ROWS.length; r++) {
      Object t = st.invoke(null, "t" + r, List.of(m.readTree(ROWS[r])));
      out.append("synthType.row").append(r).append('=').append(fields(t, ROWS[r])).append('\n');
    }
    out.append("synthType.empty=").append(fields(st.invoke(null, "e", List.of()), "{\"k\":1}")).append('\n');
    return out.toString();
  }

  /** The synthesized type's name and the type of each field of the sample row. */
  static String fields(Object type, String rowJson) throws Exception {
    com.hitorro.jsontypesystem.Type t = (com.hitorro.jsontypesystem.Type) type;
    StringBuilder b = new StringBuilder(t.getName()).append(':');
    for (String name : new String[]{"k", "n", "d", "b", "missing"}) {
      com.hitorro.jsontypesystem.Field f = t.getField(name);
      b.append(name).append('=').append(f == null ? "-" : String.valueOf(f.getMetaNode().get("type"))).append(';');
    }
    return b.toString();
  }

  @Test
  void matchesJackson2() throws Exception {
    Map<String, String> golden = new LinkedHashMap<>();
    try (InputStream in = getClass().getResourceAsStream("jackson2-golden.txt")) {
      for (String line : new String(in.readAllBytes(), StandardCharsets.UTF_8).split("\n")) {
        int eq = line.indexOf('=');
        if (eq > 0) golden.put(line.substring(0, eq), line.substring(eq + 1));
      }
    }
    Map<String, String> actual = new LinkedHashMap<>();
    for (String line : generate().split("\n")) {
      int eq = line.indexOf('=');
      if (eq > 0) actual.put(line.substring(0, eq), line.substring(eq + 1));
    }
    List<String> diffs = new ArrayList<>();
    for (String k : golden.keySet())
      if (!Objects.equals(golden.get(k), actual.get(k))) diffs.add(k + ": jackson2=" + golden.get(k) + " now=" + actual.get(k));
    for (String k : actual.keySet()) if (!golden.containsKey(k)) diffs.add(k + " (not in golden)");
    org.junit.jupiter.api.Assertions.assertTrue(diffs.isEmpty(), diffs.size() + " differences:\n" + String.join("\n", diffs));
    org.junit.jupiter.api.Assertions.assertEquals(345, golden.size());
  }
}
