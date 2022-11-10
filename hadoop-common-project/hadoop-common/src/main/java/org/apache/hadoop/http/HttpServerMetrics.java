package org.apache.hadoop.http;

import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;

@Metrics(about = "Aggregate Http Server metrics", context = "httpserver")
public class HttpServerMetrics {
  final HttpServer2 server;
  final MetricsRegistry registry;
  final String name;

  HttpServerMetrics(HttpServer2 server) {
    name = "HttpServer";
    this.server = server;
    registry = new MetricsRegistry("httpserver");
  }

  public static HttpServerMetrics create(HttpServer2 server) {
    HttpServerMetrics m = new HttpServerMetrics(server);
    return DefaultMetricsSystem.instance().register(m.name, null, m);
  }

  @Metric("Number of threads in the pool")
  public int getThreads() {
    return server.getThreadPool().getThreads();
  }

  @Metric("Number of threads ready to execute transient jobs")
  public int getReadyThreads() {
    return server.getThreadPool().getReadyThreads();
  }

  @Metric("Number of threads used by internal components")
  public int getLeasedThreads() {
    return server.getThreadPool().getLeasedThreads();
  }

  @Metric("Number of idle threads but not reserved")
  public int getIdleThreads() {
    return server.getThreadPool().getIdleThreads();
  }

  @Metric("number of threads executing internal and transient jobs")
  public int getBusyThreads() {
    return server.getThreadPool().getBusyThreads();
  }

  @Metric("number of threads executing transient jobs")
  public int getUtilizedThreads() {
    return server.getThreadPool().getUtilizedThreads();
  }

  @Metric("Maximum number of threads available to run transient jobs")
  public int getMaxAvailableThreads() {
    return server.getThreadPool().getMaxAvailableThreads();
  }

}
