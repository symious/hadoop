package org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy;

import java.util.Map;

public interface SchedulableEntityFilter<S extends SchedulableEntity> {

  public boolean filter(S s);

  public void configure(Map<String, String> conf);
}
