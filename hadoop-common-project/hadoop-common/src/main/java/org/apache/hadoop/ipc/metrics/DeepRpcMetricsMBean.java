package org.apache.hadoop.ipc.metrics;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import javax.management.openmbean.CompositeData;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface DeepRpcMetricsMBean {

  public int getCurrentFreeDeepHandlerCount();
  public CompositeData getCurrentDeepQueueSizes();
  public CompositeData getDeepCallsByNamespace();
  public CompositeData getCurrentDeepHandlerUtilization();
}
