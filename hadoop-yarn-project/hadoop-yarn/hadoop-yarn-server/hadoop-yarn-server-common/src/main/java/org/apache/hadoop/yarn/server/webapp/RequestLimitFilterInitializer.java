package org.apache.hadoop.yarn.server.webapp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.FilterContainer;
import org.apache.hadoop.http.FilterInitializer;

import java.util.HashMap;
import java.util.Map;

public class RequestLimitFilterInitializer extends FilterInitializer {

  public static String prefix = "yarn.http.request.";

  public RequestLimitFilterInitializer() {
  }

  protected Map<String, String> createFilterConfig(Configuration conf) {
    Map<String, String> filterParams = new HashMap<String, String>();
    for (Map.Entry<String, String> entry : conf.getValByRegex(prefix)
        .entrySet()) {
      String name = entry.getKey();
      String value = entry.getValue();
      name = name.substring(prefix.length());
      filterParams.put(name, value);
    }
    return filterParams;
  }

  @Override
  public void initFilter(FilterContainer container, Configuration conf) {
    Map<String, String> filterConfig = createFilterConfig(conf);
    container.addGlobalFilter("Request Limit Filter", RequestLimitFilter.class.getName(),
        filterConfig);
  }
}
