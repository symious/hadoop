package org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation;

import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.server.api.ContainerLogContext;

public class LongRunningContainerLogAggregationPolicy
    extends AbstractContainerLogAggregationPolicy {

  private long launchTime;

  @Override
  public void parseParameters(String parameters) {

  }

  @Override
  public boolean shouldDoLogAggregation(ContainerLogContext logContext) {
    if (Time.now() - logContext.getContainerLaunchTime() > 1800000) {
      return true;
    }
    return false;
  }
}
