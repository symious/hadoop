package org.apache.hadoop.yarn.server.nodemanager;

import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class NodeManagerLabelTracker {

  private static final Logger LOG = LoggerFactory.getLogger(NodeManagerLabelTracker.class);
  private String nodeLabel;

  public static final String DEFAULT = "default";

  private String processLabels;

  private static final String PRESTO_PROCESS = "io.trino.server.TrinoServer";
  private static final String RSS_PROCESS = "com.aliyun.emr.rss.service.deploy.worker.Worker";
  private static final String DataNode_PROCESS = "org.apache.hadoop.hdfs.server.datanode.DataNode";

  private static final String PRESTO = "PRESTO";
  private static final String RSS = "RSS";
  private static final String DataNode = "DataNode";

  private String jmxPrometheusConfigContent = "rules:\n" +
      "  - pattern: 'Hadoop<service=NodeManager, name=(.*)><>(.*): (\\d+)'\n" +
      "    name: 'Hadoop_NodeManager_$2'\n" +
      "    labels:\n" +
      "      yarn_cluster_id: '%s'\n" +
      "      nmlabel: '%s'\n" +
      "      name: '$1'\n" +
      "      process: '%s'\n" +
      "  - pattern: 'java.lang<type=OperatingSystem><>(.*): (\\d+)'\n" +
      "    name: 'java_lang_OperatingSystem_$1'\n" +
      "    labels:\n" +
      "      yarn_cluster_id: '%s'\n" +
      "      nmlabel: '%s'\n" +
      "      name: '$1'\n" +
      "      process: '%s'\n" +
      "  - pattern: '.*'";

  private String jmxPrometheusConfig;
  private String clusterId;

  public NodeManagerLabelTracker(String jmxPrometheusConfig, String clusterId) {
    this.jmxPrometheusConfig = jmxPrometheusConfig;
    this.clusterId =  clusterId;
  }


  public void setNodeLabel(String label) {
    if (StringUtils.isNullOrEmpty(label)) {
      this.nodeLabel = DEFAULT;
    } else {
      this.nodeLabel = label;
    }

    this.processLabels = getProcessLabels();

    // Write to Prometheus jmx config
    BufferedWriter bufferWritter = null;
    try {
      File file = new File(jmxPrometheusConfig);
      if (file.exists()) {
        FileWriter fileWritter = new FileWriter(file, false);
        bufferWritter = new BufferedWriter(fileWritter);
        bufferWritter.write(
            String.format(jmxPrometheusConfigContent, this.clusterId, this.nodeLabel,
                this.processLabels, this.clusterId, this.nodeLabel, this.processLabels));
      }
    } catch (Exception e) {
      LOG.error("Write error. ", e);
    } finally {
      if (bufferWritter != null) {
        try {
          bufferWritter.close();
        } catch (IOException e) {
          LOG.error("Write Streaming Close Error. ", e);
        }
      }
    }
  }

  private String getProcessLabels() {
    // Check Process
    StringBuilder processLabelStrBuilder = new StringBuilder("YARN");

    if (isProcessRunning(PRESTO_PROCESS)) {
      processLabelStrBuilder.append("_" + PRESTO);
    }
    if (isProcessRunning(RSS_PROCESS)) {
      processLabelStrBuilder.append("_" + RSS);
    }
    if (isProcessRunning(DataNode_PROCESS)) {
      processLabelStrBuilder.append("_" + DataNode);
    }

    return processLabelStrBuilder.toString();
  }

  public String getNodeLabel() {
    return this.nodeLabel;
  }

  public String getDefaultNodeLabel() {
    return DEFAULT;
  }

  private boolean isProcessRunning(String processName) {
    boolean isProcessRunning = false;
    String command = String.format("ps -ef|grep %s |grep -v grep", processName);
    Process p = null;
    BufferedReader reader = null;
    try {
      String[] commands = {"/bin/sh", "-c", command};
      if (Shell.LINUX) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("command is: " + Arrays.toString(commands));
        }
        p = Runtime.getRuntime().exec(commands);
        p.waitFor(5000, TimeUnit.MILLISECONDS);
        reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
        String line;
        while ((line = reader.readLine()) != null) {
          if (line.contains(processName)) {
            isProcessRunning = true;
            break;
          }
        }
      }
    } catch (Exception e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("check process shell error, ", e);
      }
    } finally {
      if (null != reader) {
        try {
          reader.close();
        } catch (IOException e) {
          LOG.error("Close reader:", e);
        }
      }
      if (null != p) {
        p.destroy();
      }
    }
    return isProcessRunning;
  }
}
