package org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy;

import java.util.Map;

public class EnvFilter<S extends SchedulableEntity>
    implements SchedulableEntityFilter<S> {

  private Map<String, String> filterEnvs;

  @Override
  public boolean filter(S s) {
    if (s.getAppSubmissionContext() == null ||
          s.getAppSubmissionContext().getAMContainerSpec() == null) {
        return false;
    }
    Map<String, String> amEnvs =
        s.getAppSubmissionContext().getAMContainerSpec().getEnvironment();
    for (Map.Entry<String, String> entry : filterEnvs.entrySet()) {
      if (!amEnvs.containsKey(entry.getKey()) || !amEnvs.get(entry.getKey())
          .equals(entry.getValue()))
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
