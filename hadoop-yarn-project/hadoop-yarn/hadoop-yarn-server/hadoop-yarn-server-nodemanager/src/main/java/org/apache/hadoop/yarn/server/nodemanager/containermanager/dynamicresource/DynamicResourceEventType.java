package org.apache.hadoop.yarn.server.nodemanager.containermanager.dynamicresource;

public enum DynamicResourceEventType {
  PUBLISH_UPDATE_CONTAINER_REQUEST,
  EXECUTE_INCREASE_RESOURCE,
  STOP_APPLICATION,
}
