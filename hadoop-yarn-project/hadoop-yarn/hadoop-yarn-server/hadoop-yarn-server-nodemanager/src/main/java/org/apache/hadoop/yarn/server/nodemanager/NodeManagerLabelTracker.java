package org.apache.hadoop.yarn.server.nodemanager;

import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class NodeManagerLabelTracker {

  private static final Logger LOG = LoggerFactory.getLogger(NodeManagerLabelTracker.class);
  private String nodeLabel;

  public static final String DEFAULT = "default";

  private String jmxPrometheusConfigContent = "rules:\n" +
      "  - pattern: 'Hadoop<service=NodeManager, name=(.*)><>(.*): (\\d+)'\n" +
      "    name: 'Hadoop_NodeManager_$2'\n" +
      "    labels:\n" +
      "      nmlabel: '%s'\n" +
      "      name: '$1'\n" +
      "  - pattern: '.*'";

  private String jmxPrometheusConfig;

  public NodeManagerLabelTracker(String jmxPrometheusConfig) {
    this.jmxPrometheusConfig = jmxPrometheusConfig;
  }


  public void setNodeLabel(String label) {
    if (StringUtils.isNullOrEmpty(label)) {
      this.nodeLabel = DEFAULT;
    } else {
      this.nodeLabel = label;
    }
    BufferedWriter bufferWritter = null;
    try {
      File file =new File(jmxPrometheusConfig);
      if (file.exists()) {
        FileWriter fileWritter = new FileWriter(file, false);
        bufferWritter = new BufferedWriter(fileWritter);
        bufferWritter.write(String.format(jmxPrometheusConfigContent, this.nodeLabel));
      }
    }catch (Exception e) {
      LOG.error("Write error. ",e);
    }finally {
      if (bufferWritter!=null) {
        try {
          bufferWritter.close();
        } catch (IOException e) {
          LOG.error("Write Streaming Close Error. ",e);
        }
      }
    }
  }

  public String getNodeLabel() {
    return this.nodeLabel;
  }

  public String getDefaultNodeLabel() {
    return DEFAULT;
  }
}
