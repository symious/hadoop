package org.apache.hadoop.yarn.server.nodemanager.containermanager.dynamicresource;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.UpdateContainerRequest;
import org.apache.hadoop.yarn.api.records.UpdatedContainer;
import org.apache.hadoop.yarn.event.AbstractEvent;

public class DynamicResourceEvent
    extends AbstractEvent<DynamicResourceEventType> {

  private ApplicationId appId;
  private UpdateContainerRequest updateContainerRequest;
  private UpdatedContainer updatedContainer;

  public DynamicResourceEvent(DynamicResourceEventType type, ApplicationId appId) {
    super(type);
    this.appId = appId;
  }

  public DynamicResourceEvent(DynamicResourceEventType type, ApplicationId appId,
      UpdateContainerRequest request) {
    super(type);
    this.appId = appId;
    this.updateContainerRequest = request;
  }

  public DynamicResourceEvent(DynamicResourceEventType type,
      ApplicationId appId, UpdatedContainer updatedContainer) {
    super(type);
    this.appId = appId;
    this.updatedContainer = updatedContainer;
  }

  public ApplicationId getApplicationId() {
    return appId;
  }

  public UpdateContainerRequest getUpdateContainerRequest() {
    return updateContainerRequest;
  }

  public UpdatedContainer getUpdatedContainer() {
    return updatedContainer;
  }
}
