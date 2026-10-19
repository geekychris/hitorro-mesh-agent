<p align="center">
  <img src="assets/logo.png" alt="Hitorro Mesh Agent logo" width="180">
</p>

# Hitorro Mesh Agent

Per-JVM worker for Hitorro Mesh — hosts LocalTable partitions of JVS data, subscribes to task dispatches over MeshTransport, executes scan/pushdown/aggregate stages via TaskExecutor, tracks watermarks for streaming tables, publishes heartbeats, and streams result rows back to the driver.
