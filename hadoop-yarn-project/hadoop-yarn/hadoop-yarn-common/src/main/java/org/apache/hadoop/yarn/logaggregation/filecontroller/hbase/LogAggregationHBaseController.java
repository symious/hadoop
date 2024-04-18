package org.apache.hadoop.yarn.logaggregation.filecontroller.hbase;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogKey;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogValue;
import org.apache.hadoop.yarn.logaggregation.ContainerLogAggregationType;
import org.apache.hadoop.yarn.logaggregation.LogToolUtils;
import org.apache.hadoop.yarn.logaggregation.filecontroller.hbase.AggregatedLogHBaseFormat.LogWriter;
import org.apache.hadoop.yarn.logaggregation.filecontroller.hbase.AggregatedLogHBaseFormat.LogReader;
import org.apache.hadoop.yarn.logaggregation.ContainerLogMeta;
import org.apache.hadoop.yarn.logaggregation.ContainerLogsRequest;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationDFSException;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileController;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileControllerContext;
import org.apache.hadoop.yarn.webapp.View;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class LogAggregationHBaseController
    extends LogAggregationFileController {

  private static final Logger LOG =
      LoggerFactory.getLogger(LogAggregationHBaseController.class);

  public static final String TABLE_NAME_CONF_NAME = "yarn.log.table.name";
  public static final String DEFAULT_TABLE_NAME = "log";

  private static Object writerLock = new Object();
  private static Object readerLock = new Object();
  private static HBaseWriter hbaseWriter;
  private static HBaseReader hbaseReader;

  private LogWriter writer;

  public LogAggregationHBaseController() {
  }

  @Override
  protected void initInternal(Configuration conf) {
    synchronized (writerLock) {
      if (hbaseWriter == null) {
        try {
          hbaseWriter = new HBaseWriter(conf);
        } catch (IOException e) {
          LOG.warn(
              "LogAggregationHBaseController fail to init HBase connection.");
        }
      }
    }
  }

  private void initHBaseReader() throws IOException {
    synchronized (readerLock) {
      if (hbaseReader == null) {
        hbaseReader = new HBaseReader(conf);
      }
    }
  }

  @Override
  public void initializeWriter(LogAggregationFileControllerContext context)
      throws IOException {
    if (hbaseWriter == null) {
      throw new IOException("hbaseWriter is null");
    }
    this.writer = new LogWriter();
    this.writer.initialize(context.getAppId(), hbaseWriter);
  }

  @Override
  public void closeWriter() throws LogAggregationDFSException {
    this.writer.close();
  }

  @Override
  public void write(LogKey logKey, LogValue logValue) throws IOException {
    this.writer.append(logKey, logValue);
  }

  @Override
  public void postWrite(LogAggregationFileControllerContext record)
      throws Exception {
  }

  @Override
  public boolean isHBaseBackend() {
    return true;
  }

  @Override
  public boolean readAggregatedLogs(ContainerLogsRequest logRequest,
      OutputStream os) throws IOException {
    initHBaseReader();

    if (os == null) {
      os = System.out;
    }
    ApplicationId appId = logRequest.getAppId();
    ContainerId containerId =
        ContainerId.fromString(logRequest.getContainerId());

    LogReader reader = new LogReader(appId, containerId, hbaseReader);

    List<String> needToFetchFiles = reader.readContainerFileList();
    if (logRequest.getLogTypes() != null && !logRequest.getLogTypes()
        .isEmpty()) {
      needToFetchFiles = (List) CollectionUtils
          .intersection(needToFetchFiles, logRequest.getLogTypes());
    }
    byte[] buf = new byte[65535];
    for (String fileName : needToFetchFiles) {
      Pair<String, Long> fileMeta = reader.readContainerFileMetaData(fileName);
      long length = fileMeta.getSecond();
      LogToolUtils
          .outputContainerLog(logRequest.getContainerId(), "", fileName, length,
              logRequest.getBytes(), "",
              reader.getFileInputStream(fileName, length), os, buf,
              ContainerLogAggregationType.AGGREGATED,
              logRequest.getStartIndex(), false);
    }
    return true;
  }

  @Override
  public List<ContainerLogMeta> readAggregatedLogsMeta(
      ContainerLogsRequest logRequest) throws IOException {
    initHBaseReader();

    List<ContainerLogMeta> containersLogMeta = new ArrayList<>();
    ApplicationId appId = logRequest.getAppId();
    ContainerId containerId =
        ContainerId.fromString(logRequest.getContainerId());

    LogReader reader = new LogReader(appId, containerId, hbaseReader);
    ContainerLogMeta containerLogMeta =
        new ContainerLogMeta(containerId.toString(), "");
    Iterator<String> iter = reader.readContainerFileList().iterator();
    while (iter.hasNext()) {
      String fileName = iter.next();
      Pair<String, Long> logMeta = reader.readContainerFileMetaData(fileName);
      containerLogMeta
          .addLogMeta(logMeta.getFirst(), logMeta.getSecond() + "", "");
    }
    containersLogMeta.add(containerLogMeta);
    return containersLogMeta;
  }

  @Override
  public void renderAggregatedLogsBlock(HtmlBlock.Block html,
      View.ViewContext context) {
    try {
      initHBaseReader();
    } catch (IOException e) {
      html.h1()
          .__("LogAggregationHBaseController fail to init HBase connection.")
          .__();
      return;
    }

    HBaseAggregatedLogsBlock block =
        new HBaseAggregatedLogsBlock(context, conf, hbaseReader);
    block.render(html);
  }

  @Override
  public String getApplicationOwner(Path aggregatedLogPath, ApplicationId appId)
      throws IOException {
    return null;
  }

  @Override
  public Map<ApplicationAccessType, String> getApplicationAcls(
      Path aggregatedLogPath, ApplicationId appId) throws IOException {
    return null;
  }

  @Override
  public void verifyAndCreateRemoteLogDir() {
  }

  @Override
  public void createAppDir(final String user, final ApplicationId appId,
      UserGroupInformation userUgi) {
  }

  public static TableName getTableName(Configuration conf) {
    String tableSchemaPrefix =
        conf.get(YarnConfiguration.TIMELINE_SERVICE_HBASE_SCHEMA_PREFIX_NAME,
            YarnConfiguration.DEFAULT_TIMELINE_SERVICE_HBASE_SCHEMA_PREFIX);
    String tableName = conf.get(TABLE_NAME_CONF_NAME, DEFAULT_TABLE_NAME);
    return TableName.valueOf(tableSchemaPrefix + tableName);
  }

  public static Configuration getHBaseConf(Configuration conf)
      throws IOException {
    if (conf == null) {
      throw new NullPointerException();
    }

    Configuration hbaseConf;
    String timelineServiceHBaseConfFilePath =
        conf.get(YarnConfiguration.TIMELINE_SERVICE_HBASE_CONFIGURATION_FILE);

    if (timelineServiceHBaseConfFilePath != null
        && timelineServiceHBaseConfFilePath.length() > 0) {
      // create a clone so that we don't mess with out input one
      hbaseConf = new Configuration(conf);
      Configuration plainHBaseConf = new Configuration(false);
      Path hbaseConfigPath = new Path(timelineServiceHBaseConfFilePath);
      try (FileSystem fs = FileSystem
          .newInstance(hbaseConfigPath.toUri(), conf); FSDataInputStream in = fs
          .open(hbaseConfigPath)) {
        plainHBaseConf.addResource(in);
        HBaseConfiguration.merge(hbaseConf, plainHBaseConf);
      }
    } else {
      // default to what is on the classpath
      hbaseConf = HBaseConfiguration.create(conf);
    }
    return hbaseConf;
  }

  public static class HBaseWriter {
    private Connection conn;
    private BufferedMutator table;
    private ScheduledExecutorService flusher;

    public HBaseWriter(Configuration conf) throws IOException {
      Configuration hbaseConf = getHBaseConf(conf);
      conn = ConnectionFactory.createConnection(hbaseConf);
      table = conn.getBufferedMutator(getTableName(hbaseConf));
      flusher = Executors.newSingleThreadScheduledExecutor();
      flusher.scheduleAtFixedRate(new Runnable() {
        @Override
        public void run() {
          try {
            table.flush();
          } catch (IOException e) {
            LOG.error("exception during writer flush!", e);
          }
        }
      }, 5, 5, TimeUnit.SECONDS);
    }

    public void write(byte[] rowKey, byte[] cf, byte[] qualifier, byte[] value)
        throws IOException {
      Put p = new Put(rowKey);
      p.addColumn(cf, qualifier, value);
      table.mutate(p);
    }
  }

  public static class HBaseReader {
    private Connection conn;
    private Table table;

    public HBaseReader(Configuration conf) throws IOException {
      Configuration hbaseConf = getHBaseConf(conf);
      conn = ConnectionFactory.createConnection(hbaseConf);
      table = conn.getTable(getTableName(hbaseConf));
    }

    public byte[] get(byte[] rowKey, byte[] cf, byte[] qualifier)
        throws IOException {
      Get get = new Get(rowKey);
      get.addColumn(cf, qualifier);
      Result result = table.get(get);
      if (result == null || result.isEmpty()) {
        return null;
      }
      return result.value();
    }
  }
}
