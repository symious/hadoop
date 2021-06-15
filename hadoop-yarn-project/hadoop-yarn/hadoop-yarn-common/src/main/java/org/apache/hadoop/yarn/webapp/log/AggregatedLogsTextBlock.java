package org.apache.hadoop.yarn.webapp.log;

import com.google.inject.Inject;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.logaggregation.ContainerLogsRequest;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileController;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileControllerFactory;
import org.apache.hadoop.yarn.webapp.MimeType;
import org.apache.hadoop.yarn.webapp.view.TextView;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.apache.hadoop.yarn.webapp.YarnWebParams.APP_OWNER;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.CONTAINER_ID;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.CONTAINER_LOG_TYPE;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.NM_NODENAME;

@InterfaceAudience.LimitedPrivate({"YARN", "MapReduce"})
public class AggregatedLogsTextBlock extends TextView {

  private final LogAggregationFileControllerFactory factory;

  @Inject
  AggregatedLogsTextBlock(Configuration conf) {
    super(null, MimeType.TEXT);
    factory = new LogAggregationFileControllerFactory(conf);
  }

  @Override
  public void render() {

    String containerIdStr = $(CONTAINER_ID);
    if (StringUtils.isBlank(containerIdStr)) {
      this.writer().println("Cannot get container logs without a ContainerId");
      return;
    }
    ContainerId containerId = ContainerId.fromString(containerIdStr);
    ApplicationId applicationId = containerId.getApplicationAttemptId()
        .getApplicationId();
    String nodeIdStr = $(NM_NODENAME);
    if (StringUtils.isBlank(nodeIdStr)) {
      this.writer().println("Cannot get container logs without a NodeId");
      return;
    }

    String appOwner = $(APP_OWNER);
    if (StringUtils.isBlank(appOwner)) {
      this.writer().println("Cannot get container logs without an app owner");
      return;
    }

    Set<String> logTypeSet = null;
    String logType = $(CONTAINER_LOG_TYPE);
    if (StringUtils.isNotBlank(logType)) {
      String[] logTypes = logType.split(",");
      logTypeSet = new HashSet<>();
      for (String type : logTypes) {
        logTypeSet.add(type.trim());
      }
    }

    String start = this.request().getParameter("start");
    long startIndex = StringUtils.isNotBlank(start) ? Long.parseLong(start) : 0;
    String size = this.request().getParameter("size");
    long outputSize = StringUtils.isNotBlank(size)
        ? Long.parseLong(size)
        : Long.MAX_VALUE;

    try {
      ContainerLogsRequest options = new ContainerLogsRequest();
      options.setAppId(applicationId);
      options.setContainerId(containerIdStr);
      options.setNodeId(nodeIdStr);
      options.setAppOwner(appOwner);
      options.setLogTypes(logTypeSet);
      options.setBytes(outputSize);
      options.setStartIndex(startIndex);

      LogAggregationFileController controller =
          factory.getFileControllerForRead(applicationId, appOwner);
      controller.readAggregatedLogs(options, this.outputStream());
    } catch (IOException e) {
      this.writer().println(e.getMessage());
    }
  }
}
