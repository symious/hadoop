package org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation;

import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.server.api.ContainerLogAggregationPolicy;
import org.apache.hadoop.yarn.server.api.ContainerLogContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class CompositeLogAggregationPolicy
    extends AbstractContainerLogAggregationPolicy {

  private static final Logger LOG =
      LoggerFactory.getLogger(CompositeLogAggregationPolicy.class);
  private List<ContainerLogAggregationPolicy> policyList = new ArrayList<>();

  @Override
  public boolean shouldDoLogAggregation(ContainerLogContext logContext) {
    for (ContainerLogAggregationPolicy policy : policyList) {
      if (policy.shouldDoLogAggregation(logContext)) {
       return true;
      }
    }
    return false;
  }

  @Override
  public void parseParameters(String parameters) {
    Collection<String> params = StringUtils.getStringCollection(parameters);
    ContainerLogAggregationPolicy policy = null;
    Class<? extends ContainerLogAggregationPolicy> clazz = null;
    for (String param : params) {
      switch (param.toUpperCase()) {
        case "AMONLYLOGAGGREGATIONPOLICY":
          clazz = AMOnlyLogAggregationPolicy.class;
          break;
        case "AMORFAILEDCONTAINERLOGAGGREGATIONPOLICY":
          clazz = AMOrFailedContainerLogAggregationPolicy.class;
          break;
        case "FAILEDAPPLICATIONLOGAGGREGATIONPOLICY":
          clazz = FailedApplicationLogAggregationPolicy.class;
          break;
        case "FAILEDCONTAINERLOGAGGREGATIONPOLICY":
          clazz = FailedContainerLogAggregationPolicy.class;
          break;
        case "FAILEDORKILLEDCONTAINERLOGAGGREGATIONPOLICY":
          clazz = FailedOrKilledContainerLogAggregationPolicy.class;
          break;
        case "LIMITSIZECONTAINERLOGAGGREGATIONPOLICY":
          clazz = LimitSizeContainerLogAggregationPolicy.class;
          break;
      }
      policy = ReflectionUtils.newInstance(clazz, null);
      policyList.add(policy);
    }
    if (policyList.isEmpty()) {
      policy = ReflectionUtils
          .newInstance(AllContainerLogAggregationPolicy.class, null);
      policyList.add(policy);
    }
    LOG.info("Container Log Aggregation Policy list: " + policyList);
  }
}
