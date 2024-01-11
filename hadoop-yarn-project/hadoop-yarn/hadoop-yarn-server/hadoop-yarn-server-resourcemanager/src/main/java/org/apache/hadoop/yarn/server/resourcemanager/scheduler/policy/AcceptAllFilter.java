package org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy;

import java.util.Map;

public class AcceptAllFilter<S extends SchedulableEntity>
    implements SchedulableEntityFilter<S> {
  @Override
  public boolean filter(SchedulableEntity schedulableEntity) {
    return true;
  }

  @Override
  public void configure(Map conf) {
  }

  public String toString() {
    return "[type: " + AcceptAllFilter.class.getName() + "]";
  }
}
