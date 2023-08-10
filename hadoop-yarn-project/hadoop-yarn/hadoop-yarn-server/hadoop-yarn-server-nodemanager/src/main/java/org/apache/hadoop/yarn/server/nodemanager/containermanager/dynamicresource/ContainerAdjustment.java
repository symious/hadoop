package org.apache.hadoop.yarn.server.nodemanager.containermanager.dynamicresource;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerUpdateType;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;

public class ContainerAdjustment {
  private ContainerId containerId;
  private Container container;
  private ContainerUpdateType updateType;
  private Resource originalResource;
  protected long deltaMemory;

  public ContainerAdjustment(ContainerId containerId, Container container,
      ContainerUpdateType updateType, Resource originalRes, long delta) {
    this.containerId = containerId;
    this.container = container;
    this.updateType = updateType;
    this.originalResource = originalRes;
    this.deltaMemory = delta;
  }

  public ContainerUpdateType getContainerUpdateType() {
    return updateType;
  }

  public Container getContainer() {
    return container;
  }

  public ContainerId getContainerId() {
    return containerId;
  }

  public Resource getOriginalResource() {
    return originalResource;
  }

  public String toString() {
    return "containerId: " + containerId + " updateType: " + updateType + " deltaMemory: " + deltaMemory;
  }
}
