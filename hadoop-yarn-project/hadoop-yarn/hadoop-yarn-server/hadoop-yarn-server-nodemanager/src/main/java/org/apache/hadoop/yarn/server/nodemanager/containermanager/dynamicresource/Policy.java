package org.apache.hadoop.yarn.server.nodemanager.containermanager.dynamicresource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.UpdateContainerRequest;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;

public interface Policy {

  ContainerAdjustment apply(Container container);

  void init(Configuration conf);
}
