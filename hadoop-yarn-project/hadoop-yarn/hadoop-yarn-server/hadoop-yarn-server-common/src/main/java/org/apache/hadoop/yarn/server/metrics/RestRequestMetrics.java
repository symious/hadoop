package org.apache.hadoop.yarn.server.metrics;


import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.metrics2.lib.Interns.info;

@InterfaceAudience.Private
@Metrics(context = "yarn")
public class RestRequestMetrics {
  private static AtomicBoolean isInitialized = new AtomicBoolean(false);

  private static final MetricsInfo RECORD_INFO =
      info("RestRequestMetrics",
          "Metrics for the Yarn Rest Request");

  private static volatile RestRequestMetrics INSTANCE = null;
  private static MetricsRegistry registry;

  public static RestRequestMetrics getMetrics() {
    if (!isInitialized.get()) {
      synchronized (RestRequestMetrics.class) {
        if (INSTANCE == null) {
          INSTANCE = new RestRequestMetrics();
          registerMetrics();
          isInitialized.set(true);
        }
      }
    }
    return INSTANCE;
  }

  private static void registerMetrics() {
    registry = new MetricsRegistry(RECORD_INFO);
    MetricsSystem ms = DefaultMetricsSystem.instance();
    if (ms != null) {
      ms.register("RestRequestMetrics",
          "Metrics for the Yarn Rest Request", INSTANCE);
    }
  }


  @Metric("RM Rest API request count")
  MutableGaugeLong restAPICount;
  @Metric("RM Rest Metrics API request count")
  MutableGaugeLong metricsAPICount;
  @Metric("RM Rest Scheduler API request count")
  MutableGaugeLong schedulerAPICount;
  @Metric("RM Rest Nodes API request count")
  MutableGaugeLong nodesAPICount;
  @Metric("RM Rest Apps API request count")
  MutableGaugeLong appsAPICount;
  @Metric("RM Rest Containers API request count")
  MutableGaugeLong containersAPICount;


  public long getRestAPICount() {
    return restAPICount.value();
  }

  public void incrRestAPICount() {
    restAPICount.incr();
  }

  public long getMetricsAPICount() {
    return metricsAPICount.value();
  }

  public void incrMetricsAPICount() {
    metricsAPICount.incr();
  }

  public long getSchedulerAPICount() {
    return schedulerAPICount.value();
  }

  public void incrSchedulerAPICount() {
    schedulerAPICount.incr();
  }

  public long getNodesAPICount() {
    return nodesAPICount.value();
  }

  public void incrNodesAPICount() {
    nodesAPICount.incr();
  }

  public long getAppsAPICount() {
    return appsAPICount.value();
  }

  public void incrAppsAPICount() {
    appsAPICount.incr();
  }

  public long getContainersAPICount() {
    return containersAPICount.value();
  }

  public void incrContainersAPICount() {
    containersAPICount.incr();
  }

}
