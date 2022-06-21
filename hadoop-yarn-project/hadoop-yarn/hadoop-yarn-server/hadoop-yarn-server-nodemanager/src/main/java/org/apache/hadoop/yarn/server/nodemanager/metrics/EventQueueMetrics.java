package org.apache.hadoop.yarn.server.nodemanager.metrics;

import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;

@Metrics(about = "Metrics for node manager event queue", context = "yarn")
public class EventQueueMetrics {

  @Metric("Count for NM event")
  MutableGaugeInt nmEventQueueSize;

  @Metric("Count for ContainerManager event")
  MutableGaugeInt nmContainerManagerQueueSize;

  @Metric("Count for Timeline event")
  MutableGaugeLong nmTimelineQueueSize;

  private EventQueueMetrics() {
  }

  public static EventQueueMetrics create() {
    return create(DefaultMetricsSystem.instance());
  }

  private static EventQueueMetrics create(MetricsSystem ms) {
    return ms.register(new EventQueueMetrics());
  }

  public MutableGaugeInt getNmEventQueueSize() {
    return nmEventQueueSize;
  }

  public void setNMEventQueueSize(
      int nmEventQueueSize) {
    this.nmEventQueueSize.set(nmEventQueueSize);
  }

  public MutableGaugeInt getNmContainerManagerQueueSize() {
    return nmContainerManagerQueueSize;
  }

  public void setNmContainerManagerQueueSize(
      int nmContainerManagerEventQueueSize) {
    this.nmContainerManagerQueueSize.set(nmContainerManagerEventQueueSize);
  }

  public MutableGaugeLong getNmTimelineQueueSize() {
    return nmTimelineQueueSize;
  }

  public void setnmTimelineQueueSize(
      int nmTimelineQueueSize) {
    this.nmTimelineQueueSize.set(nmTimelineQueueSize);
  }

}
