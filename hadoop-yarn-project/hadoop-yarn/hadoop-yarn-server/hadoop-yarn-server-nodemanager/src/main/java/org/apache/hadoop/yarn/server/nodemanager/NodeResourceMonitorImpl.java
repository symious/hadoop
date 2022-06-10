/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.hadoop.yarn.server.nodemanager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.SysInfo;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdateNodeResourceRequest;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.gpu.GpuNodeResourceUpdateHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.gpu.GpuResourcePlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.scheduler.ContainerScheduler;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.util.ResourceCalculatorPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of the node resource monitor. It periodically tracks the
 * resource utilization of the node and reports it to the NM.
 */
public class NodeResourceMonitorImpl extends AbstractService implements
    NodeResourceMonitor {

  /** Logging infrastructure. */
  final static Logger LOG =
       LoggerFactory.getLogger(NodeResourceMonitorImpl.class);

  /** Interval to monitor the node resource utilization. */
  private long monitoringInterval;
  /** Thread to monitor the node resource utilization. */
  private MonitoringThread monitoringThread;

  /** Resource calculator. */
  private ResourceCalculatorPlugin resourceCalculatorPlugin;

  /** Gpu related plugin. */
  private GpuResourcePlugin gpuResourcePlugin;
  private GpuNodeResourceUpdateHandler gpuNodeResourceUpdateHandler;

  /** Current <em>resource utilization</em> of the node. */

  private Map<String, Float> customResources = new HashMap<>();

  private ResourceUtilization nodeUtilization =
      ResourceUtilization.newInstance(0, 0, 0f, customResources);
  private Context nmContext;

  private boolean cGroupsEnabled;
  private String cGroupsMountPath;
  private boolean highLoadStrategyEnabled;

  private long lastCheck = 0;

  private int highLoadCheckInterval;

  private float load1Threshold;
  private float load5Threshold;

  /**
   * Initialize the node resource monitor.
   */
  public NodeResourceMonitorImpl(Context context) {
    super(NodeResourceMonitorImpl.class.getName());
    this.nmContext = context;
    this.monitoringThread = new MonitoringThread();
  }

  /**
   * Initialize the service with the proper parameters.
   */
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    this.monitoringInterval =
        conf.getLong(YarnConfiguration.NM_RESOURCE_MON_INTERVAL_MS,
            YarnConfiguration.DEFAULT_NM_RESOURCE_MON_INTERVAL_MS);

    Class executorClass = conf.getClass(YarnConfiguration.NM_CONTAINER_EXECUTOR,
        DefaultContainerExecutor.class, ContainerExecutor.class);
    if (executorClass.getName().equals(LinuxContainerExecutor.class.getName())) {
      this.cGroupsEnabled = true;
      this.cGroupsMountPath =
          conf.get(YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_MOUNT_PATH, null);
      this.highLoadStrategyEnabled =
          conf.getBoolean(YarnConfiguration.NM_HIGH_LOAD_CPU_USAGE_LIMIT_ENABLED,
              YarnConfiguration.DEFAULT_NM_HIGH_LOAD_CPU_USAGE_LIMIT_ENABLED);
      this.load1Threshold = conf.getFloat(YarnConfiguration.NM_HIGH_LOAD1_THRESHOLD,
          YarnConfiguration.DEFAULT_NM_HIGH_LOAD1_THRESHOLD);
      this.load5Threshold = conf.getFloat(YarnConfiguration.NM_HIGH_LOAD5_THRESHOLD,
          YarnConfiguration.DEFAULT_NM_HIGH_LOAD5_THRESHOLD);
      this.lastCheck = System.currentTimeMillis();
      this.highLoadCheckInterval = conf.getInt(YarnConfiguration.NM_HIGH_LOAD_CHECK_INTERVAL_MS,
          YarnConfiguration.DEFAULT_NM_HIGH_LOAD_CHECK_INTERVAL_MS);
    }

    this.resourceCalculatorPlugin =
        ResourceCalculatorPlugin.getNodeResourceMonitorPlugin(conf);

    if (nmContext.getResourcePluginManager() != null) {
      this.gpuResourcePlugin =
          (GpuResourcePlugin)nmContext.getResourcePluginManager().
          getNameToPlugins().get(ResourceInformation.GPU_URI);

      if (gpuResourcePlugin != null) {
        this.gpuNodeResourceUpdateHandler =
            (GpuNodeResourceUpdateHandler)gpuResourcePlugin.
                getNodeResourceHandlerInstance();
      }
    }

    LOG.info(" Using ResourceCalculatorPlugin : "
        + this.resourceCalculatorPlugin);
  }

  /**
   * Check if we should be monitoring.
   * @return <em>true</em> if we can monitor the node resource utilization.
   */
  private boolean isEnabled() {
    if (this.monitoringInterval <= 0) {
      LOG.info("Node Resource monitoring interval is <=0. "
          + this.getClass().getName() + " is disabled.");
      return false;
    }
    if (resourceCalculatorPlugin == null) {
      LOG.info("ResourceCalculatorPlugin is unavailable on this system. "
          + this.getClass().getName() + " is disabled.");
      return false;
    }
    return true;
  }

  /**
   * Start the thread that does the node resource utilization monitoring.
   */
  @Override
  protected void serviceStart() throws Exception {
    if (this.isEnabled()) {
      this.monitoringThread.start();
    }
    super.serviceStart();
  }

  /**
   * Stop the thread that does the node resource utilization monitoring.
   */
  @Override
  protected void serviceStop() throws Exception {
    if (this.isEnabled()) {
      this.monitoringThread.interrupt();
      try {
        this.monitoringThread.join(10 * 1000);
      } catch (InterruptedException e) {
        LOG.warn("Could not wait for the thread to join");
      }
    }
    super.serviceStop();
  }

  /**
   * Thread that monitors the resource utilization of this node.
   */
  private class MonitoringThread extends Thread {
    /**
     * Initialize the node resource monitoring thread.
     */
    public MonitoringThread() {
      super("Node Resource Monitor");
      this.setDaemon(true);
    }

    /**
     * Periodically monitor the resource utilization of the node.
     */
    @Override
    public void run() {
      while (true) {
        // Get node utilization and save it into the health status
        long pmem = resourceCalculatorPlugin.getPhysicalMemorySize() -
            resourceCalculatorPlugin.getAvailablePhysicalMemorySize();
        long vmem =
            resourceCalculatorPlugin.getVirtualMemorySize()
                - resourceCalculatorPlugin.getAvailableVirtualMemorySize();
        float vcores = resourceCalculatorPlugin.getNumVCoresUsed();

        float totalNodeGpuUtilization = 0F;
        try {
          if (gpuNodeResourceUpdateHandler != null) {
            totalNodeGpuUtilization =
                gpuNodeResourceUpdateHandler.getTotalNodeGpuUtilization();
          }
        } catch (Exception e) {
          LOG.error("Get Node GPU Utilization error: " + e);
        }

        customResources.
            put(ResourceInformation.GPU_URI, totalNodeGpuUtilization);
        nodeUtilization =
            ResourceUtilization.newInstance(
                (int) (pmem >> 20), // B -> MB
                (int) (vmem >> 20), // B -> MB
                vcores,     // Used Virtual Cores
                customResources);  // Used GPUs

        // Publish the node utilization metrics to node manager
        // metrics system.
        NodeManagerMetrics nmMetrics = nmContext.getNodeManagerMetrics();
        if (nmMetrics != null) {
          nmMetrics.setNodeUsedMemGB(nodeUtilization.getPhysicalMemory());
          nmMetrics.setNodeUsedVMemGB(nodeUtilization.getVirtualMemory());
          nmMetrics.setNodeCpuUtilization(nodeUtilization.getCPU());
          nmMetrics.setNodeGpuUtilization(totalNodeGpuUtilization);
        }

        boolean canUpdateContainersResource = false;
        if ((lastCheck + highLoadCheckInterval) < System.currentTimeMillis()) {
          canUpdateContainersResource = true;
        }
        // Clean leak CGroups containers config
        if (cGroupsEnabled && highLoadStrategyEnabled && canUpdateContainersResource) {
          lastCheck = System.currentTimeMillis();
          // Clean the leak container file under CGroups
          cleanLeakContainers();
          // Node Load Balance strategy
          updateContainersResource();
          // Update the total resource of CGroups
          updateTotalCGroupsResource();
        }
        try {
          Thread.sleep(monitoringInterval);
        } catch (InterruptedException e) {
          LOG.warn(NodeResourceMonitorImpl.class.getName()
              + " is interrupted. Exiting.");
          break;
        }
      }
    }
  }

  private void updateTotalCGroupsResource() {
    try {
      ContainerScheduler scheduler = nmContext.getContainerManager().getContainerScheduler();
      scheduler.updateTotalCGroupsResource();
    } catch (Exception e) {
      LOG.error("ERROR from updateTotalCGroupsResource: ", e);
    }
  }

  private void cleanLeakContainers() {
    try {
      ContainerScheduler scheduler = nmContext.getContainerManager().getContainerScheduler();
      scheduler.cleanLeakContainers();
    } catch (Exception e) {
      LOG.error("ERROR from cleanLeakContainers: ", e);
    }
  }

  private void updateContainersResource() {
    try {
      boolean isHighLoad = false;
      SysInfo info = SysInfo.newInstance();
      if ((info.getLoad1() / info.getNumProcessors()) > load1Threshold) {
        isHighLoad = true;
      }

      if ((info.getLoad5() / info.getNumProcessors()) > load5Threshold) {
        isHighLoad = true;
      }

      ContainerScheduler scheduler = nmContext.getContainerManager().getContainerScheduler();
      scheduler.updateContainersByLoad(isHighLoad);
    } catch (Exception e) {
      LOG.error("ERROR from UpdateContainerResource: ", e);
    }
  }

  /**
   * Get the <em>resource utilization</em> of the node.
   * @return <em>resource utilization</em> of the node.
   */
  @Override
  public ResourceUtilization getUtilization() {
    return this.nodeUtilization;
  }

  @Override
  public void updateNodeResource(int coreNumber, long memory) throws Exception {
    ResourceManagerAdministrationProtocol adminProtocol =  ClientRMProxy.createRMProxy(this.nmContext.getConf(),
        ResourceManagerAdministrationProtocol.class);
    RecordFactory recordFactory =
        RecordFactoryProvider.getRecordFactory(null);
    UpdateNodeResourceRequest request =
        recordFactory.newRecordInstance(UpdateNodeResourceRequest.class);
    NodeId nodeId = this.nmContext.getNodeId();

    Map<NodeId, ResourceOption> resourceMap =
        new HashMap<NodeId, ResourceOption>();
    resourceMap.put(
        nodeId, ResourceOption.newInstance(Resource.newInstance(memory, coreNumber), 0));
    request.setNodeResourceMap(resourceMap);
    adminProtocol.updateNodeResource(request);
    // Update Config as well
    Configuration conf = this.getConfig();
    SysInfo info = SysInfo.newInstance();
    int coreRatio = (coreNumber * 100) / info.getNumProcessors();
    if (coreRatio < 1) {
      coreRatio = 1;
    }
    if (coreRatio > 100) {
      coreRatio = 100;
    }
    conf.setInt(YarnConfiguration.NM_RESOURCE_PERCENTAGE_PHYSICAL_CPU_LIMIT, coreRatio);
    conf.setInt(YarnConfiguration.NM_PMEM_MB, Long.valueOf(memory).intValue());
    conf.setInt(YarnConfiguration.NM_VCORES, coreNumber);
    if (LOG.isDebugEnabled()) {
      LOG.debug("New Resources: Mem-" + memory + ", Vcore-" + coreNumber + ", Ratio-" + coreRatio);
    }
  }
}
