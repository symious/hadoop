/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.util.NodeManagerHardwareUtils;
import org.apache.hadoop.yarn.util.ResourceCalculatorPlugin;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * An implementation for using CGroups to restrict CPU usage on Linux. The
 * implementation supports 3 different controls - restrict usage of all YARN
 * containers, restrict relative usage of individual YARN containers and
 * restrict usage of individual YARN containers. Admins can set the overall CPU
 * to be used by all YARN containers - this is implemented by setting
 * cpu.cfs_period_us and cpu.cfs_quota_us to the ratio desired. If strict
 * resource usage mode is not enabled, cpu.shares is set for individual
 * containers - this prevents containers from exceeding the overall limit for
 * YARN containers but individual containers can use as much of the CPU as
 * available(under the YARN limit). If strict resource usage is enabled, then
 * container can only use the percentage of CPU allocated to them and this is
 * again implemented using cpu.cfs_period_us and cpu.cfs_quota_us.
 *
 */
@InterfaceStability.Unstable
@InterfaceAudience.Private
public class CGroupsCpuResourceHandlerImpl implements CpuResourceHandler {

  static final Logger LOG =
       LoggerFactory.getLogger(CGroupsCpuResourceHandlerImpl.class);

  private CGroupsHandler cGroupsHandler;
  private boolean strictResourceUsageMode = false;
  private boolean strictResourceUsageModeWithSoftLimit = false;
  private int maxStrictCoreNumber = 10;

  private int defaultStrictCoreNumber = 10;

  private String STRICT_CORE_NUMBER = "STRICT_CORE_NUMBER";

  private float criticalLimitFactor = 1f;
  private float highLimitFactor = 1f;
  private float mediumLimitFactor = 1f;
  private float lowLimitFactor = 1f;

  private float criticalShareLimitFactor = 1f;
  private float highShareLimitFactor = 1f;
  private float mediumShareLimitFactor = 1f;
  private float lowShareLimitFactor = 1f;

  private float yarnProcessors;
  private int nodeVCores;
  private static final CGroupsHandler.CGroupController CPU =
      CGroupsHandler.CGroupController.CPU;

  @VisibleForTesting
  static final int MAX_QUOTA_US = 1000 * 1000;
  @VisibleForTesting
  static final int MIN_PERIOD_US = 1000;
  @VisibleForTesting
  static final int CPU_DEFAULT_WEIGHT = 1024; // set by kernel
  static final int CPU_DEFAULT_WEIGHT_OPPORTUNISTIC = 2;

  CGroupsCpuResourceHandlerImpl(CGroupsHandler cGroupsHandler) {
    this.cGroupsHandler = cGroupsHandler;
  }

  @Override
  public List<PrivilegedOperation> bootstrap(Configuration conf)
      throws ResourceHandlerException {
    return bootstrap(
        ResourceCalculatorPlugin.getResourceCalculatorPlugin(null, conf), conf);
  }

  @VisibleForTesting
  List<PrivilegedOperation> bootstrap(
      ResourceCalculatorPlugin plugin, Configuration conf)
      throws ResourceHandlerException {
    this.strictResourceUsageMode = conf.getBoolean(
        YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_STRICT_RESOURCE_USAGE,
        YarnConfiguration.DEFAULT_NM_LINUX_CONTAINER_CGROUPS_STRICT_RESOURCE_USAGE);
    this.strictResourceUsageModeWithSoftLimit =
        conf.getBoolean(
            YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_STRICT_RESOURCE_USAGE_WITH_SOFT,
            YarnConfiguration.DEFAULT_NM_LINUX_CONTAINER_CGROUPS_STRICT_RESOURCE_USAGE_WITH_SOFT);
    this.maxStrictCoreNumber = conf.getInt(
        YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_STRICT_RESOURCE_USAGE_WITH_SOFT_MAX_STRICT_CORE_NUMBER,
        YarnConfiguration.DEFAULT_NM_LINUX_CONTAINER_CGROUPS_STRICT_RESOURCE_USAGE_WITH_STRICT_CORE_NUMBER);

    this.defaultStrictCoreNumber = conf.getInt(
        YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_STRICT_RESOURCE_USAGE_WITH_SOFT_DEFAULT_STRICT_CORE_NUMBER,
        YarnConfiguration.DEFAULT_NM_LINUX_CONTAINER_CGROUPS_STRICT_RESOURCE_USAGE_WITH_SOFT_DEFAULT_STRICT_CORE_NUMBER);

    this.criticalLimitFactor = conf.getFloat(YarnConfiguration.NM_CONTAINER_LEVEL_CRITICAL_LIMIT,
        YarnConfiguration.DEFAULT_NM_CONTAINER_LEVEL_CRITICAL_LIMIT);
    this.highLimitFactor = conf.getFloat(YarnConfiguration.NM_CONTAINER_LEVEL_HIGH_LIMIT,
        YarnConfiguration.DEFAULT_NM_CONTAINER_LEVEL_HIGH_LIMIT);
    this.mediumLimitFactor = conf.getFloat(YarnConfiguration.NM_CONTAINER_LEVEL_MEDIUM_LIMIT,
        YarnConfiguration.DEFAULT_NM_CONTAINER_LEVEL_MEDIUM_LIMIT);
    this.lowLimitFactor = conf.getFloat(YarnConfiguration.NM_CONTAINER_LEVEL_LOW_LIMIT,
        YarnConfiguration.DEFAULT_NM_CONTAINER_LEVEL_LOW_LIMIT);

    this.criticalShareLimitFactor =
        conf.getFloat(YarnConfiguration.NM_CONTAINER_LEVEL_CRITICAL_SHARE_LIMIT,
            YarnConfiguration.DEFAULT_NM_CONTAINER_LEVEL_CRITICAL_SHARE_LIMIT);
    this.highShareLimitFactor = conf.getFloat(YarnConfiguration.NM_CONTAINER_LEVEL_HIGH_SHARE_LIMIT,
        YarnConfiguration.DEFAULT_NM_CONTAINER_LEVEL_HIGH_SHARE_LIMIT);
    this.mediumShareLimitFactor =
        conf.getFloat(YarnConfiguration.NM_CONTAINER_LEVEL_MEDIUM_SHARE_LIMIT,
            YarnConfiguration.DEFAULT_NM_CONTAINER_LEVEL_MEDIUM_SHARE_LIMIT);
    this.lowShareLimitFactor = conf.getFloat(YarnConfiguration.NM_CONTAINER_LEVEL_LOW_SHARE_LIMIT,
        YarnConfiguration.DEFAULT_NM_CONTAINER_LEVEL_LOW_SHARE_LIMIT);

    this.cGroupsHandler.initializeCGroupController(CPU);
    nodeVCores = NodeManagerHardwareUtils.getVCores(plugin, conf);

    // cap overall usage to the number of cores allocated to YARN
    yarnProcessors = NodeManagerHardwareUtils.getContainersCPUs(plugin, conf);
    int systemProcessors = NodeManagerHardwareUtils.getNodeCPUs(plugin, conf);
    boolean existingCpuLimits;
    try {
      existingCpuLimits =
          cpuLimitsExist(cGroupsHandler.getPathForCGroup(CPU, ""));
    } catch (IOException ie) {
      throw new ResourceHandlerException(ie);
    }
    if (systemProcessors != (int) yarnProcessors) {
      LOG.info("YARN containers restricted to " + yarnProcessors + " cores");
      int[] limits = getOverallLimits(yarnProcessors);
      cGroupsHandler
          .updateCGroupParam(CPU, "", CGroupsHandler.CGROUP_CPU_PERIOD_US,
              String.valueOf(limits[0]));
      cGroupsHandler
          .updateCGroupParam(CPU, "", CGroupsHandler.CGROUP_CPU_QUOTA_US,
              String.valueOf(limits[1]));
    } else if (existingCpuLimits) {
      LOG.info("Removing CPU constraints for YARN containers.");
      cGroupsHandler
          .updateCGroupParam(CPU, "", CGroupsHandler.CGROUP_CPU_QUOTA_US,
              String.valueOf(-1));
    }
    return null;
  }

  @InterfaceAudience.Private
  public static boolean cpuLimitsExist(String path)
      throws IOException {
    File quotaFile = new File(path,
        CPU.getName() + "." + CGroupsHandler.CGROUP_CPU_QUOTA_US);
    if (quotaFile.exists()) {
      String contents = FileUtils.readFileToString(quotaFile, "UTF-8");
      int quotaUS = Integer.parseInt(contents.trim());
      if (quotaUS != -1) {
        return true;
      }
    }
    return false;
  }

  @VisibleForTesting
  @InterfaceAudience.Private
  public static int[] getOverallLimits(float yarnProcessors) {

    int[] ret = new int[2];

    if (yarnProcessors < 0.01f) {
      throw new IllegalArgumentException("Number of processors can't be <= 0.");
    }

    int quotaUS = MAX_QUOTA_US;
    int periodUS = (int) (MAX_QUOTA_US / yarnProcessors);
    if (yarnProcessors < 1.0f) {
      periodUS = MAX_QUOTA_US;
      quotaUS = (int) (periodUS * yarnProcessors);
      if (quotaUS < MIN_PERIOD_US) {
        LOG.warn("The quota calculated for the cgroup was too low."
            + " The minimum value is " + MIN_PERIOD_US
            + ", calculated value is " + quotaUS
            + ". Setting quota to minimum value.");
        quotaUS = MIN_PERIOD_US;
      }
    }

    // cfs_period_us can't be less than 1000 microseconds
    // if the value of periodUS is less than 1000, we can't really use cgroups
    // to limit cpu
    if (periodUS < MIN_PERIOD_US) {
      LOG.warn("The period calculated for the cgroup was too low."
          + " The minimum value is " + MIN_PERIOD_US
          + ", calculated value is " + periodUS
          + ". Using all available CPU.");
      periodUS = MAX_QUOTA_US;
      quotaUS = -1;
    }

    ret[0] = periodUS;
    ret[1] = quotaUS;
    return ret;
  }

  @Override
  public List<PrivilegedOperation> preStart(Container container)
      throws ResourceHandlerException {
    String cgroupId = container.getContainerId().toString();
    cGroupsHandler.createCGroup(CPU, cgroupId);
    updateContainer(container);
    List<PrivilegedOperation> ret = new ArrayList<>();
    ret.add(new PrivilegedOperation(
        PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP,
        PrivilegedOperation.CGROUP_ARG_PREFIX + cGroupsHandler
            .getPathForCGroupTasks(CPU, cgroupId)));
    return ret;
  }

  @Override
  public List<PrivilegedOperation> reacquireContainer(ContainerId containerId)
      throws ResourceHandlerException {
    return null;
  }

  @Override
  public List<PrivilegedOperation> updateContainer(Container container)
      throws ResourceHandlerException {
    Resource containerResource = container.getResource();
    String cgroupId = container.getContainerId().toString();
    File cgroup = new File(cGroupsHandler.getPathForCGroup(CPU, cgroupId));
    if (cgroup.exists()) {
      try {
        int containerVCores = containerResource.getVirtualCores();
        ContainerTokenIdentifier id = container.getContainerTokenIdentifier();
        if (id != null && id.getExecutionType() ==
            ExecutionType.OPPORTUNISTIC) {
          cGroupsHandler
              .updateCGroupParam(CPU, cgroupId,
                  CGroupsHandler.CGROUP_CPU_SHARES,
                  String.valueOf(CPU_DEFAULT_WEIGHT_OPPORTUNISTIC));
        } else {
          int cpuShares = CPU_DEFAULT_WEIGHT * containerVCores;
          // If encounter high load then adjust the ShareValue
          // To ensure the high level container CPU usage
          if (container.isHighLoad()) {
            float shareFactor = getShareFactorByContainerLevel(container.getContainerLevel());
            cpuShares = Double.valueOf(cpuShares * shareFactor).intValue();
          }
          cGroupsHandler
              .updateCGroupParam(CPU, cgroupId,
                  CGroupsHandler.CGROUP_CPU_SHARES,
                  String.valueOf(cpuShares));
        }


        LOG.debug("Container Id: " + container.getContainerId().toString() +
            " Container vcores: " + containerVCores);
        if (strictResourceUsageMode) {
          setupLimitsInternal(containerVCores, cgroupId);
        } else if (strictResourceUsageModeWithSoftLimit) {
          // Get STRICT CORE NUMBER from Env
          int strictCoreNumber = 0;
          String strictCoreString =
              container.getLaunchContext().getEnvironment()
                  .get(STRICT_CORE_NUMBER);

          // Set the strictCoreNumber according to the app level
          strictCoreNumber = getStrictCoreNumberByContainerLevel(container.getContainerLevel(),
              container.getResource().getVirtualCores());
          // If user set this param and less than the allowed value then use it
          if (!StringUtils.isNullOrEmpty(strictCoreString)) {
            int strictCoreNumberFromUser = Integer.valueOf(strictCoreString);
            if (strictCoreNumberFromUser < strictCoreNumber) {
              strictCoreNumber = strictCoreNumberFromUser;
            }
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug("nodeVCores is: " + nodeVCores + ", containerVCores: " + containerVCores +
                ", yarnProcessors: " + yarnProcessors + ", strictCoreNumber: " + strictCoreNumber);
          }
          // If overload will change to let it less than max
          if (strictCoreNumber > nodeVCores) {
            strictCoreNumber = nodeVCores;
          }

          containerVCores = strictCoreNumber;
          setupLimitsInternal(containerVCores, cgroupId);
        }
      } catch (ResourceHandlerException re) {
        cGroupsHandler.deleteCGroup(CPU, cgroupId);
        LOG.warn("Could not update cgroup for container", re);
        throw re;
      }
    }
    return null;
  }

  private int getStrictCoreNumberByContainerLevel(String containerLevel, int virtualCores) {
    if (!StringUtils.isNullOrEmpty(containerLevel)) {
      float factor = 1;
      switch (containerLevel) {
        case "CRITICAL":
          factor = this.criticalLimitFactor;
          break;
        case "HIGH":
          factor = this.highLimitFactor;
          break;
        case "MEDIUM":
          factor = this.mediumLimitFactor;
          break;
        case "LOW":
          factor = this.lowLimitFactor;
          break;
        default:
          break;
      }
      return Math.round(virtualCores * factor);
    }
    return this.defaultStrictCoreNumber;
  }

  private float getShareFactorByContainerLevel(String containerLevel) {
    float factor = 1;
    if (!StringUtils.isNullOrEmpty(containerLevel)) {
      switch (containerLevel) {
        case "CRITICAL":
          factor = this.criticalShareLimitFactor;
          break;
        case "HIGH":
          factor = this.highShareLimitFactor;
          break;
        case "MEDIUM":
          factor = this.mediumShareLimitFactor;
          break;
        case "LOW":
          factor = this.lowShareLimitFactor;
          break;
        default:
          break;
      }
    }
    return factor;
  }

  private void setupLimitsInternal(int containerVCores, String cgroupId)
      throws ResourceHandlerException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("nodeVCores is: " + nodeVCores + ", containerVCores: " + containerVCores +
          ", yarnProcessors: " + yarnProcessors);
    }
    cGroupsHandler.updateCGroupParam(CPU, cgroupId,
        CGroupsHandler.CGROUP_CPU_PERIOD_US, String.valueOf(MAX_QUOTA_US));
    cGroupsHandler.updateCGroupParam(CPU, cgroupId,
        CGroupsHandler.CGROUP_CPU_QUOTA_US, "-1");
    if (nodeVCores != containerVCores) {
      float containerCPU =
          (containerVCores * yarnProcessors) / (float) nodeVCores;
      int[] limits = getOverallLimits(containerCPU);
      cGroupsHandler.updateCGroupParam(CPU, cgroupId,
          CGroupsHandler.CGROUP_CPU_PERIOD_US, String.valueOf(limits[0]));
      cGroupsHandler.updateCGroupParam(CPU, cgroupId,
          CGroupsHandler.CGROUP_CPU_QUOTA_US, String.valueOf(limits[1]));
    }
  }

  @Override
  public List<PrivilegedOperation> postComplete(ContainerId containerId)
      throws ResourceHandlerException {
    cGroupsHandler.deleteCGroup(CPU, containerId.toString());
    return null;
  }

  @Override public List<PrivilegedOperation> teardown()
      throws ResourceHandlerException {
    return null;
  }

  @Override
  public String toString() {
    return CGroupsCpuResourceHandlerImpl.class.getName();
  }

  @Override
  public void cleanLeakContainers(Set<String> containerIDs) throws IOException {
    this.cGroupsHandler.cleanLeakContainers(containerIDs);
  }

  @Override
  public void updateTotalCGroupsResource(Configuration conf) throws ResourceHandlerException {
    ResourceCalculatorPlugin plugin =
        ResourceCalculatorPlugin.getResourceCalculatorPlugin(null, conf);
    nodeVCores = NodeManagerHardwareUtils.getVCores(plugin, conf);
    yarnProcessors = NodeManagerHardwareUtils.getContainersCPUs(plugin, conf);
    LOG.info("YARN containers restricted to " + yarnProcessors + " cores");
    int[] limits = getOverallLimits(yarnProcessors);
    cGroupsHandler
        .updateCGroupParam(CPU, "", CGroupsHandler.CGROUP_CPU_PERIOD_US,
            String.valueOf(limits[0]));
    cGroupsHandler
        .updateCGroupParam(CPU, "", CGroupsHandler.CGROUP_CPU_QUOTA_US,
            String.valueOf(limits[1]));
  }
}
