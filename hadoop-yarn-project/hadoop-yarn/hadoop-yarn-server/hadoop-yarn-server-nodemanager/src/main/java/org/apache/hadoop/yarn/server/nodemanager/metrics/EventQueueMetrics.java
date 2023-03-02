package org.apache.hadoop.yarn.server.nodemanager.metrics;

import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;

@Metrics(about = "Metrics for node manager event queue", context = "yarn")
public class EventQueueMetrics {

  private Context context = null;

  private EventQueueMetrics() {
  }

  public static EventQueueMetrics create() {
    return create(DefaultMetricsSystem.instance());
  }

  private static EventQueueMetrics create(MetricsSystem ms) {
    return ms.register(new EventQueueMetrics());
  }

  public void setNmContext(Context context) {
    this.context = context;
  }

  @Metric(type = Metric.Type.COUNTER)
  public int getNMEventQueueSize() {
    if (context != null) {
      return ((AsyncDispatcher) context.getDispatcher())
          .getCurrentEventQueueSize();
    }
    return 0;
  }

  @Metric(type = Metric.Type.COUNTER)
  public int getNMContainerManagerQueueSize() {
    if (context != null) {
      return ((ContainerManagerImpl) context.getContainerManager())
          .getDispatcher().getCurrentEventQueueSize();
    }
    return 0;
  }

  @Metric(type = Metric.Type.COUNTER)
  public int getNMTimelineQueueSize() {
    if (context != null && context.getNMTimelinePublisher() != null) {
      return context.getNMTimelinePublisher().getDispatcher()
          .getCurrentEventQueueSize();
    }
    return 0;
  }
}
