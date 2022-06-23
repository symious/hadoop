package org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation;

import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.server.api.ContainerLogContext;

public class FailedApplicationLogAggregationPolicy
    extends AbstractContainerLogAggregationPolicy {

  @Override
  public boolean shouldDoLogAggregation(ContainerLogContext logContext) {
    return logContext.getYarnApplicationState() == null
        || logContext.getYarnApplicationState() == YarnApplicationState.FAILED
        || logContext.getYarnApplicationState() == YarnApplicationState.KILLED;
  }
}
