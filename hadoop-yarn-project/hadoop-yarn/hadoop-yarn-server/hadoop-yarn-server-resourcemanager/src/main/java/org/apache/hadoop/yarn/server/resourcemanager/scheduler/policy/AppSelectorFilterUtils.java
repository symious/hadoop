package org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy;

import java.util.Map;

public class AppSelectorFilterUtils {
  private static final String LOW_PRIORITY = "low_priority";
  private static final String ENV = "env";

  public static <S extends SchedulableEntity> SchedulableEntityFilter<S> getFilter(
      String name, Map<String, String> config) {
    String filterType;
    switch (name) {
      case LOW_PRIORITY:
        filterType = LowPriorityFilter.class.getName();
        break;
      case ENV:
        filterType = EnvFilter.class.getName();
        break;
      default:
        filterType = AcceptAllFilter.class.getName();
    }
    SchedulableEntityFilter<S> filter;
    try {
      filter =
          (SchedulableEntityFilter<S>) Class.forName(filterType).newInstance();
      filter.configure(config);
    } catch (Exception e) {
      String message =
          "Unable to construct application selector's filter for: " + name
              + ", " + e.getMessage();
      throw new RuntimeException(message, e);
    }
    return filter;
  }
}