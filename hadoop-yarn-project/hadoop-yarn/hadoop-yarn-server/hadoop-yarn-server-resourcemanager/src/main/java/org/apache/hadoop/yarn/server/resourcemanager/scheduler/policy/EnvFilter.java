package org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy;

import java.util.HashMap;
import java.util.Map;

public class EnvFilter<S extends SchedulableEntity>
    implements SchedulableEntityFilter<S> {

  private Map<String, String> filterEnvs = new HashMap<>();

  @Override
  public boolean filter(S s) {
    Map<String, String> amEnvs = s.getApplicationSchedulingEnvs();
    // if filterEnvs is not subset of amEnvs, return false
    if (!amEnvs.entrySet().containsAll(filterEnvs.entrySet())) {
      return false;
    }
    return true;
  }

  @Override
  public void configure(Map<String, String> conf) {
    this.filterEnvs = conf;
  }

  public String toString() {
    return "[type: " + EnvFilter.class.getName() + ", param: " + filterEnvs
        .toString() + "]";
  }
}
