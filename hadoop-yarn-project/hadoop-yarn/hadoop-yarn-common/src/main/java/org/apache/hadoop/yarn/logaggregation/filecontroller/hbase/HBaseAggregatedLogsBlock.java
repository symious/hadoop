package org.apache.hadoop.yarn.logaggregation.filecontroller.hbase;

import com.google.inject.Inject;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationHtmlBlock;
import org.apache.hadoop.yarn.util.Times;

import java.io.IOException;
import java.util.Iterator;

import static org.apache.hadoop.yarn.webapp.YarnWebParams.CONTAINER_LOG_TYPE;

@InterfaceAudience.LimitedPrivate({ "YARN", "MapReduce" })
public class HBaseAggregatedLogsBlock extends LogAggregationHtmlBlock {

  private final Configuration conf;
  private final LogAggregationHBaseController.HBaseReader hbaseReader;

  @Inject
  public HBaseAggregatedLogsBlock(ViewContext ctx, Configuration conf,
      LogAggregationHBaseController.HBaseReader hbaseReader) {
    super(ctx);
    this.conf = conf;
    this.hbaseReader = hbaseReader;
  }

  @Override
  protected void render(Block html) {
    BlockParameters params = verifyAndParseParameters(html);
    if (params == null) {
      return;
    }

    NodeId nodeId = params.getNodeId();
    String logEntity = params.getLogEntity();
    ApplicationId appId = params.getAppId();
    ContainerId containerId = params.getContainerId();
    long start = params.getStartIndex();
    long end = params.getEndIndex();
    long startTime = params.getStartTime();
    long endTime = params.getEndTime();

    boolean foundLog = false;
    String desiredLogType = $(CONTAINER_LOG_TYPE);
    AggregatedLogHBaseFormat.LogReader reader = null;
    try {
      reader = new AggregatedLogHBaseFormat.LogReader(appId, containerId,
          hbaseReader);
      foundLog =
          readContainerLogs(html, reader, start, end, desiredLogType, -1l,
              startTime, endTime);
    } catch (IOException ex) {
      LOG.error("Error getting logs for " + logEntity, ex);
    }
    if (!foundLog) {
      if (desiredLogType.isEmpty()) {
        html.h1("No logs available for container " + containerId.toString());
      } else {
        html.h1("Unable to locate '" + desiredLogType + "' log for container "
            + containerId.toString());
      }
    }
  }

  private boolean readContainerLogs(Block html,
      AggregatedLogHBaseFormat.LogReader reader, long startIndex, long endIndex,
      String desiredLogType, long logUpLoadTime, long startTime, long endTime)
      throws IOException {
    int bufferSize = 65536;
    byte[] cbuf = new byte[bufferSize];

    boolean foundLog = false;
    Iterator<String> iter = reader.readContainerFileList().iterator();
    while (iter.hasNext()) {
      String logType = iter.next();
      if (desiredLogType == null || desiredLogType.isEmpty() || desiredLogType
          .equals(logType)) {
        long logLength = reader.readContainerFileMetaData(logType).getSecond();
        if (foundLog) {
          html.pre().__("\n\n").__();
        }

        html.p().__("Log Type: " + logType).__();
        html.p().__("Log Upload Time: " + Times.format(logUpLoadTime)).__();
        html.p().__("Log Length: " + Long.toString(logLength)).__();

        long[] range =
            checkParseRange(html, startIndex, endIndex, startTime, endTime,
                logLength, logType);

        processContainerLog(html, range,
            reader.getFileInputStream(logType, logLength), bufferSize, cbuf);
        foundLog = true;
      }
    }
    return foundLog;
  }
}
