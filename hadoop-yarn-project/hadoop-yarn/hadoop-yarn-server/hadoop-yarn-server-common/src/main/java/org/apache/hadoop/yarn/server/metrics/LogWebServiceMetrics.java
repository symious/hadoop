package org.apache.hadoop.yarn.server.metrics;

import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.metrics2.lib.Interns.info;

@Metrics(about = "Metrics for Log service", context = "logservice")
public class LogWebServiceMetrics {

  private final static MetricsInfo METRICS_INFO = info("LogServiceMetrics",
      "Metrics for LogService");
  private static AtomicBoolean isInitialized = new AtomicBoolean(false);
  private static LogWebServiceMetrics instance = null;

  @Metric(about = "Get logs latency", valueName = "latency", interval = 120)
  private MutableQuantiles getLogsLatency;

  @Metric(about = "Read HDFS latency", valueName = "latency", interval = 120)
  private MutableQuantiles readHDFSLatency;

  @VisibleForTesting
  protected LogWebServiceMetrics(){
  }

  public static LogWebServiceMetrics getInstance() {
    if (!isInitialized.get()) {
      synchronized (LogWebServiceMetrics.class) {
        if (instance == null) {
          instance = DefaultMetricsSystem.initialize("TimelineService").register(
              METRICS_INFO.name(), METRICS_INFO.description(),
              new LogWebServiceMetrics());
          isInitialized.set(true);
        }
      }
    }
    return instance;
  }

  public void addGetLogsLatency(long durationMs) {
      getLogsLatency.add(durationMs);
  }

  public void addReadHDFSLatency(long durationMs) {
    readHDFSLatency.add(durationMs);
  }

}
