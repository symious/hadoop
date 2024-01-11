package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.AppSelectorFilterUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.AppSelector;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.SchedulableEntityFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class AppSelectorManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(AppSelectorManager.class);

  private static final String APP_SELECTOR_PREFIX =
      CapacitySchedulerConfiguration.PREFIX + "app-selectors";
  private static final String FILTERS = "filters";
  private static final String PARAMS = "params";

  private CapacitySchedulerConfiguration conf;
  private Map<String, AppSelector> partitionToAppSelector = new HashMap<>();

  // yarn.scheduler.capacity.app-selectors = colocation,night-shift
  // yarn.scheduler.capacity.app-selectors.colocation.filters = env,priority
  // yarn.scheduler.capacity.app-selectors.colocation.filters.env.params = rssEnabled=true

  public void initialize(CapacitySchedulerConfiguration conf) {
    this.conf = conf;
    this.partitionToAppSelector = getPartitionToAppSelector();
    LOG.info("Initialized appSelector: " + partitionToAppSelector);
  }

  private Map<String, AppSelector> getPartitionToAppSelector() {
    Map<String, AppSelector> mappings = new HashMap<>();
    String[] partitions = conf.getTrimmedStrings(APP_SELECTOR_PREFIX);
    for (String partitionName : partitions) {
      AppSelector selector = getAppSelector(partitionName);
      if (selector != null && !mappings.containsKey(partitionName)) {
        mappings.put(partitionName, selector);
      }
    }
    return mappings;
  }

  private AppSelector getAppSelector(String partitionName) {
    AppSelector selector = new AppSelector();
    String[] filters = conf.getTrimmedStrings(
        APP_SELECTOR_PREFIX + "." + partitionName + "." + FILTERS);
    for (String filterName : filters) {
      selector.addFilter(filterName, getFilter(partitionName, filterName));
    }
    return selector;
  }

  private SchedulableEntityFilter getFilter(String partition,
      String filterName) {
    String[] filterParams = conf.getTrimmedStrings(
        APP_SELECTOR_PREFIX + "." + partition + "." + FILTERS + "." + filterName
            + "." + PARAMS);
    Map<String, String> filterConfig = new HashMap<>();
    for (String param : filterParams) {
      String[] keyValue = StringUtils.getStrings(param, "=");
      filterConfig.put(keyValue[0], keyValue[1]);
    }
    return AppSelectorFilterUtils.getFilter(filterName, filterConfig);
  }

  public AppSelector getMappedAppSelector(String partition) {
    return partitionToAppSelector.get(partition);
  }
}
