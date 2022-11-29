package org.apache.hadoop.yarn.server.nodemanager.containermanager.dynamicresource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.api.records.UpdateContainerRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.ContainerType;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DynamicResourceController extends Thread {

  private final static Logger LOG =
      LoggerFactory.getLogger(DynamicResourceController.class);

  private Context context;
  private Policy policy;
  private long monitoringInterval;

  public DynamicResourceController(Configuration conf, Context context) {
    super("DynamicMemoryController");
    this.context = context;
    policy = getPolicy(conf);
    policy.init(conf);
    monitoringInterval =
        conf.getLong(YarnConfiguration.NM_DYNAMIC_ADJUSTMENT_INTERVAL_MS,
            YarnConfiguration.DEFAULT_NM_DYNAMIC_ADJUSTMENT_INTERVAL_MS);
  }

  private Policy getPolicy(Configuration conf) {
    Class<? extends Policy> policyClass =
        conf.getClass(YarnConfiguration.NM_DYNAMIC_ADJUSTMENT_POLICY_CLASS,
            DefaultPolicy.class, Policy.class);
    return ReflectionUtils.newInstance(policyClass, conf);
  }

  @Override
  public void run() {
    while (true) {
      for (Container container : context.getContainers().values()) {
        if (!container.isRunning() || container.getContainerTokenIdentifier()
            .getContainerType().equals(ContainerType.APPLICATION_MASTER)) {
          continue;
        }
        UpdateContainerRequest updateContainerRequest = policy.apply(container);
        if (updateContainerRequest != null) {
          LOG.info("Generate DynamicEvent. updateContainerRequest: "
              + updateContainerRequest);
          context.getDynamicResourcePublisher().publishDynamicResourceEvent(
              new DynamicResourceEvent(
                  DynamicResourceEventType.PUBLISH_UPDATE_CONTAINER_REQUEST,
                  updateContainerRequest.getContainerId()
                      .getApplicationAttemptId().getApplicationId(),
                  updateContainerRequest));
        }
      }
      try {
        Thread.sleep(monitoringInterval);
      } catch (InterruptedException e) {
        LOG.warn("{} is interrupted. Exiting.",
            DynamicResourceController.class.getName());
        break;
      }
    }
  }

}
