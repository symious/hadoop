package org.apache.hadoop.hdfs.server.zoneservice.metrics;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;

import java.util.concurrent.ConcurrentHashMap;

@Metrics(name="ZoneServiceMetrics", about="ZoneService metrics", context="ZSinternal")
public class ZoneServiceMetrics {
  final MetricsRegistry registry = new MetricsRegistry("zoneservice");

  @Metric MutableGaugeInt monitorThreadCount;
  @Metric MutableGaugeInt batchThreadCount;
  @Metric MutableCounterLong successTotalMoveCount;
  @Metric MutableCounterLong failTotalMoveCount;
  // For ZoneMover monitor thread of a namespace
  private ConcurrentHashMap<String, MutableCounterLong> nsSuccessMoveCount
      = new ConcurrentHashMap<>();
  private ConcurrentHashMap<String, MutableCounterLong> nsFailMoveCount
      = new ConcurrentHashMap<>();

  public static ZoneServiceMetrics create() {
    return DefaultMetricsSystem.instance().register(new ZoneServiceMetrics());
  }

  public void startMonitorThread() { monitorThreadCount.incr(); }
  public void stopMonitorThread() { monitorThreadCount.decr(); }
  public void startBatchThread() { batchThreadCount.incr(); }
  public void stopBatchThread() { batchThreadCount.decr(); }
  public void incrSuccessMoveCount() { successTotalMoveCount.incr(); }
  public void incrFailMoveCount() { failTotalMoveCount.incr(); }

  public void incrNSSuccessMoveCount(String ns) {
    if (ns != null) {
      MutableCounterLong mutableCounterLong =
          nsSuccessMoveCount.get(ns);
      if (mutableCounterLong == null) {
        synchronized (this) {
          String metricName =
              StringUtils.capitalize(ns + "NSSuccessMoveCount");
          mutableCounterLong = registry.newCounter(
              Interns.info(metricName, metricName), 0l);
          nsSuccessMoveCount.putIfAbsent(ns, mutableCounterLong);
        }
      }
      nsSuccessMoveCount.get(ns).incr();
    }
  }

  public void incrNSFailMoveCount(String ns) {
    if (ns != null) {
      MutableCounterLong mutableCounterLong =
          nsFailMoveCount.get(ns);
      if (mutableCounterLong == null) {
        synchronized (this) {
          String metricName =
              StringUtils.capitalize(ns + "NSFailMoveCount");
          mutableCounterLong = registry.newCounter(
              Interns.info(metricName, metricName), 0l);
          nsFailMoveCount.putIfAbsent(ns, mutableCounterLong);
        }
      }
      nsFailMoveCount.get(ns).incr();
    }
  }

  public void shutdown() { DefaultMetricsSystem.shutdown(); }
}
