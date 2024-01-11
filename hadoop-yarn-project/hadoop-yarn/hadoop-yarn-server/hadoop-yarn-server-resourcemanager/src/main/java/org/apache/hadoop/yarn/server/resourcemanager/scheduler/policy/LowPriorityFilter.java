package org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy;

import java.util.Map;

public class LowPriorityFilter<S extends SchedulableEntity>
    implements SchedulableEntityFilter<S> {

  private static final String PRIORITY_THRESHOLD = "priorityThreshold";
  private int priorityThreshold;

  @Override
  public boolean filter(S s) {
    if (s.getPriority().getPriority() <= priorityThreshold) {
      return true;
    }
    return false;
  }

  @Override
  public void configure(Map<String, String> conf) {
    if (conf.containsKey(PRIORITY_THRESHOLD)) {
      priorityThreshold = Integer.parseInt(conf.get(PRIORITY_THRESHOLD));
    }
  }

  public String toString() {
    return "[type: " + LowPriorityFilter.class.getName()
        + ", param: {priorityThreshold=" + priorityThreshold + "}]";
  }
}
