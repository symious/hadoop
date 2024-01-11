package org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy;

import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;

import java.util.HashMap;
import java.util.Map;

public class AppSelector<S extends SchedulableEntity> {

  private Map<String, SchedulableEntityFilter<S>> filterMap;

  public AppSelector() {
    this.filterMap = new HashMap<>();
  }

  public boolean accept(S s) {
    for (SchedulableEntityFilter filter : filterMap.values()) {
      if (!filter.filter(s)) {
        return false;
      }
    }
    return true;
  }

  public void addFilter(String name, SchedulableEntityFilter filter) {
    filterMap.putIfAbsent(name, filter);
  }

  public Map<String, SchedulableEntityFilter<S>> getFilterMap() {
    return filterMap;
  }

  public String toString() {
    return filterMap.toString();
  }
}
