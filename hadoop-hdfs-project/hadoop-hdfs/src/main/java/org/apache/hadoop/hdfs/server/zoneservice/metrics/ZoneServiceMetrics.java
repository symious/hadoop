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
  @Metric Long checkRecordCostTime;
  @Metric MutableCounterLong successTotalMoveCount;
  @Metric MutableCounterLong failTotalMoveCount;
  // For ZoneMover monitor thread of a namespace
  private final ConcurrentHashMap<String, MutableCounterLong> nsMonitorSuccessMoveCount
      = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, MutableCounterLong> nsMonitorFailMoveCount
      = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, MutableCounterLong> nsBatchSuccessMoveCount
      = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, MutableCounterLong> nsBatchFailMoveCount
      = new ConcurrentHashMap<>();

  public static ZoneServiceMetrics create() {
    return DefaultMetricsSystem.instance().register(new ZoneServiceMetrics());
  }

  public void startMonitorThread() { monitorThreadCount.incr(); }
  public void stopMonitorThread() { monitorThreadCount.decr(); }
  public void startBatchThread() { batchThreadCount.incr(); }
  public void stopBatchThread() { batchThreadCount.decr(); }
  public void setCheckRecordCostTime(long costTime) { checkRecordCostTime = costTime; }
  public void incrSuccessMoveCount() { successTotalMoveCount.incr(); }
  public void incrFailMoveCount() { failTotalMoveCount.incr(); }

  public void incrNSMonitorSuccessMoveCount(String ns) {
    if (ns != null) {
      MutableCounterLong mutableCounterLong =
          nsMonitorSuccessMoveCount.get(ns);
      if (mutableCounterLong == null) {
        synchronized (this) {
          String metricName =
              StringUtils.capitalize(ns + "nsMonitorSuccessMoveCount");
          mutableCounterLong = registry.newCounter(
              Interns.info(metricName, metricName), 0L);
          nsMonitorSuccessMoveCount.putIfAbsent(ns, mutableCounterLong);
        }
      }
      nsMonitorSuccessMoveCount.get(ns).incr();
    }
  }

  public void incrNSMonitorFailMoveCount(String ns) {
    if (ns != null) {
      MutableCounterLong mutableCounterLong =
          nsMonitorFailMoveCount.get(ns);
      if (mutableCounterLong == null) {
        synchronized (this) {
          String metricName =
              StringUtils.capitalize(ns + "nsMonitorFailMoveCount");
          mutableCounterLong = registry.newCounter(
              Interns.info(metricName, metricName), 0L);
          nsMonitorFailMoveCount.putIfAbsent(ns, mutableCounterLong);
        }
      }
      nsMonitorFailMoveCount.get(ns).incr();
    }
  }

  public void incrNSBatchSuccessMoveCount(String ns) {
    if (ns != null) {
      MutableCounterLong mutableCounterLong =
          nsBatchSuccessMoveCount.get(ns);
      if (mutableCounterLong == null) {
        synchronized (this) {
          String metricName =
              StringUtils.capitalize(ns + "NSBatchSuccessMoveCount");
          mutableCounterLong = registry.newCounter(
              Interns.info(metricName, metricName), 0L);
          nsBatchSuccessMoveCount.putIfAbsent(ns, mutableCounterLong);
        }
      }
      nsBatchSuccessMoveCount.get(ns).incr();
    }
  }

  public void incrNSBatchFailMoveCount(String ns) {
    if (ns != null) {
      MutableCounterLong mutableCounterLong =
          nsBatchFailMoveCount.get(ns);
      if (mutableCounterLong == null) {
        synchronized (this) {
          String metricName =
              StringUtils.capitalize(ns + "NSBatchFailMoveCount");
          mutableCounterLong = registry.newCounter(
              Interns.info(metricName, metricName), 0L);
          nsBatchFailMoveCount.putIfAbsent(ns, mutableCounterLong);
        }
      }
      nsBatchFailMoveCount.get(ns).incr();
    }
  }

  public void shutdown() { DefaultMetricsSystem.shutdown(); }
}
