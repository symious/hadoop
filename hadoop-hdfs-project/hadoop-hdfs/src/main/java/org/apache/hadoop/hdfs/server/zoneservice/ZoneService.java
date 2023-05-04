/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.zoneservice;

import org.apache.hadoop.thirdparty.com.google.common.collect.Sets;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.ReconfigurableBase;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgressMetrics;
import org.apache.hadoop.hdfs.server.zoneservice.metrics.ZoneServiceMetrics;
import org.apache.hadoop.hdfs.server.zoneservice.store.MigrationRecord;
import org.apache.hadoop.hdfs.server.zoneservice.store.Query;
import org.apache.hadoop.hdfs.server.zoneservice.store.SignalRecord;
import org.apache.hadoop.hdfs.server.zoneservice.store.StoreDriver;
import org.apache.hadoop.hdfs.server.zoneservice.utils.ZoneServiceUtil;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tracing.TraceUtils;
import org.apache.hadoop.tracing.TracerConfigurationManager;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.htrace.core.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.Semaphore;

import static org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_USER_GROUP_METRICS_PERCENTILES_INTERVALS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_METRICS_PERCENTILES_INTERVALS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ZONESERVICE_HTTP_BIND_HOST_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ZONESERVICE_HTTP_ADDRESS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ZONESERVICE_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ZONESERVICE_STORE_DRIVER_CLASS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ZONESERVICE_STORE_DRIVER_CLASS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ZONE_GENERTE_REPLICATION_RULE_ENABLED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ZONE_GENERTE_REPLICATION_RULE_ENABLED_KEY_DEFAULT;
import static org.apache.hadoop.util.ExitUtil.terminate;

public class ZoneService extends ReconfigurableBase  {
  public static final Logger LOG = LoggerFactory.getLogger(ZoneService.class);
  /** Metrics to track shadow file activity */
  static ZoneServiceMetrics metrics = ZoneServiceMetrics.create();

  // A list of property that are reconfigurable at runtime.
  private final TreeSet<String> reconfigurableProperties = Sets.newTreeSet();

  private static final String USAGE = "Usage: ";

  // httpServer
  protected ZoneServiceHttpServer httpServer;

  // store driver
  protected StoreDriver driver;
  private Thread batchManager;
  private final int nsThreadLimit;
  private final long batchRefreshInterval;
  private final HashMap<String, Semaphore> nsSemaphore = new HashMap<>();
  private final HashMap<String, List<String>> inProcessPaths = new HashMap<>();

  private static final String ZONESERVICE_HTRACE_PREFIX = "zoneservice.htrace.";

  private static final StartupProgress startupProgress = new StartupProgress();

  protected final Tracer tracer;
  protected final TracerConfigurationManager tracerConfigurationManager;
  private ReplicationRuleGenerateKafkaTrigger replicationRuleGenerateKafkaTrigger;

  public ZoneService(Configuration conf) throws IOException {
    this.tracer = new Tracer.Builder("ZoneService").
        conf(TraceUtils.wrapHadoopConf(ZONESERVICE_HTRACE_PREFIX, conf)).
        build();
    this.tracerConfigurationManager =
        new TracerConfigurationManager(ZONESERVICE_HTRACE_PREFIX, conf);
    this.nsThreadLimit = conf.getInt(
        DFSConfigKeys.DFS_ZONESERVICE_BATCH_NS_THREAD_CONSTRAINT_KEY,
        DFSConfigKeys.DFS_ZONESERVICE_BATCH_NS_THREAD_CONSTRAINT_DEFAULT);
    this.batchRefreshInterval = conf.getLong(
        DFSConfigKeys.DFS_ZONESERVICE_BATCH_THREAD_REFRESH_INTERVAL_KEY,
        DFSConfigKeys.DFS_ZONESERVICE_BATCH_THREAD_REFRESH_INTERVAL_DEFAULT);
    try {
      initialize(getConf());
    } catch (IOException | HadoopIllegalArgumentException e) {
      this.stopAtException(e);
      throw e;
    }
  }

  public static void createZoneService(String[] argv, Configuration conf)
      throws IOException {
    LOG.info("createZoneService " + Arrays.asList(argv));
    if (conf == null)
      conf = new HdfsConfiguration();

    DefaultMetricsSystem.initialize("ZoneService");
    new ZoneService(conf);
  }

  /**
   * Initialize zone-service.
   *
   * @param conf the configuration
   */
  protected void initialize(Configuration conf) throws IOException {
    if (conf.get(HADOOP_USER_GROUP_METRICS_PERCENTILES_INTERVALS) == null) {
      String intervals = conf.get(DFS_METRICS_PERCENTILES_INTERVALS_KEY);
      if (intervals != null) {
        conf.set(HADOOP_USER_GROUP_METRICS_PERCENTILES_INTERVALS,
            intervals);
      }
    }
    UserGroupInformation.setConfiguration(conf);
    StartupProgressMetrics.register(startupProgress);
    startHttpServer(conf);
    Class<? extends StoreDriver> driverClass = conf.getClass(
        DFS_ZONESERVICE_STORE_DRIVER_CLASS,
        DFS_ZONESERVICE_STORE_DRIVER_CLASS_DEFAULT,
        StoreDriver.class);
    driver = ReflectionUtils.newInstance(driverClass, conf);
    driver.init(conf, httpServer.getHttpAddress().getHostName() + ":"
        + httpServer.getHttpAddress().getPort());
    recoverZMProcess(conf);
    initReplicationRuleGenerateKafkaTrigger(conf);
  }

  /**
   * HTTP server address for binding the endpoint. This method is
   * for use by the ZoneService and its derivatives. It may return
   * a different address than the one that should be used by clients to
   * connect to the ZoneService. See
   * {@link DFSConfigKeys#DFS_ZONESERVICE_HTTP_BIND_HOST_KEY}
   *
   * @param conf configuration of zone service
   * @return return the http bind address of zone service
   */
  protected InetSocketAddress getHttpServerBindAddress(Configuration conf) {
    InetSocketAddress bindAddress = getHttpAddress(conf);

    // If DFS_ZONESERVICE_HTTP_BIND_HOST_KEY exists then it overrides the
    // host name portion of DFS_ZONESERVICE_HTTP_ADDRESS_KEY.
    final String bindHost = conf.getTrimmed(DFS_ZONESERVICE_HTTP_BIND_HOST_KEY);
    if (bindHost != null && !bindHost.isEmpty()) {
      bindAddress = new InetSocketAddress(bindHost, bindAddress.getPort());
    }

    return bindAddress;
  }

  /** @return the ZoneService HTTP address. */
  public static InetSocketAddress getHttpAddress(Configuration conf) {
    return  NetUtils.createSocketAddr(
        conf.getTrimmed(DFS_ZONESERVICE_HTTP_ADDRESS_KEY,
            DFS_ZONESERVICE_HTTP_ADDRESS_DEFAULT));
  }

  public static ZoneServiceMetrics getMetrics() {
    return metrics;
  }

  private void startHttpServer(final Configuration conf) throws IOException {
    httpServer = new ZoneServiceHttpServer(conf, getHttpServerBindAddress(conf));
    httpServer.start();
    httpServer.setStartupProgress(startupProgress);
  }

  /**
   * Stop all ZoneService threads and wait for all to finish.
   */
  public void stop() {
    batchManager.interrupt();
    stopHttpServer();
    tracer.close();
    stopReplicationRuleGenerateKafkaTrigger();
  }

  private void stopHttpServer() {
    try {
      if (httpServer != null) httpServer.stop();
    } catch (Exception e) {
      LOG.error("Exception while stopping httpserver", e);
    }
  }

  private void stopAtException(Exception e){
    try {
      this.stop();
    } catch (Exception ex) {
      LOG.warn("Encountered exception when handling exception ("
          + e.getMessage() + "):", ex);
    }
  }

  //Start unfinished batch ZoneMover for every mapping file under specific file
  private void recoverZMProcess(Configuration conf) throws IOException {
    //first remove all signal record.
    LOG.info("remove all signal record.");
    driver.removeAll(SignalRecord.class);
    Map<String, Map<String, ReplicationRule>> nsRuleMap = new HashMap<>();
    List<MigrationRecord> records =
        driver.getAll(MigrationRecord.class).getRecords();
    for (MigrationRecord record : records) {
      try {
        if (record.getNs().equals("null")) {
          driver.remove(new Query<>(record), MigrationRecord.class);
        }
        //Record monitor mode path-rule pairs
        if (record.getMode().equals("monitor")) {
          if (!nsRuleMap.containsKey(record.getNs())) {
            nsRuleMap.put(record.getNs(), new HashMap<>());
          }
          nsRuleMap.get(record.getNs()).put(record.getPath(),
              ReplicationRule.parseFromString(record.getRule()));
        }
      } catch (IllegalArgumentException e) {
        LOG.error("Record is illegal: " + record);
      }
    }
    for (String ns: nsRuleMap.keySet()) {
      SignalRecord signalRecord = new SignalRecord(ns, true);
      driver.put(signalRecord, true, false);
      LOG.info("Starting monitor thread for {}.", ns);
      Thread monitorThread = new MonitorThread("monitor_" + ns,
          conf, ZoneServiceUtil.getNamespaceUri(ns, conf), driver, signalRecord);
      monitorThread.start();
    }
    // Restart batch manager
    batchManager = new BatchManagerThread();
    batchManager.start();
  }

  class BatchManagerThread extends Thread {
    public BatchManagerThread() {
      super("BatchManager");
    }

    @Override
    public void run() {
      while (true) {
        try {
          long start = System.currentTimeMillis();
          List<MigrationRecord> records = driver.getAll(MigrationRecord.class).getRecords();
          for (MigrationRecord record : records) {
            if (record.getMode().equals("monitor")) continue;
            String ns = record.getNs();
            String path = record.getPath();
            if (!nsSemaphore.containsKey(ns)) {
              nsSemaphore.put(ns, new Semaphore(nsThreadLimit));
              inProcessPaths.put(ns, Collections.synchronizedList(new ArrayList<String>()));
            }
            if (nsSemaphore.get(ns).availablePermits() > 0
                && !inProcessPaths.get(ns).contains(path)) {
              nsSemaphore.get(ns).acquire();
              inProcessPaths.get(ns).add(path);
              (new BatchThread(path, record.getRule(),
                  ns, getConf(), driver, nsSemaphore.get(ns), inProcessPaths.get(ns))).start();
            }
            // The same timestamp will cause that the ZoneMover cannot work
            Thread.sleep(10);
          }
          metrics.addCheckRecordCostTime(System.currentTimeMillis() - start);
          Thread.sleep(batchRefreshInterval);
        } catch (IOException e) {
          LOG.error("Batch manager thread error!", e);
        } catch (InterruptedException e) {
          break;
        }
      }
    }
  }

  private URI getNamespaceUri(String namespace, Configuration conf)
      throws IllegalArgumentException {
    Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);
    for (URI namenode: namenodes) {
      if (namenode.getAuthority().equals(namespace)) {
        return namenode;
      }
    }
    throw new IllegalArgumentException(
        "Cannot find the NameNode for namespace: " + namespace);
  }

  private void initReplicationRuleGenerateKafkaTrigger(Configuration conf) throws IOException {
    boolean replicationRuleGenerateEnabled = conf.getBoolean(
        DFS_ZONE_GENERTE_REPLICATION_RULE_ENABLED_KEY,
        DFS_ZONE_GENERTE_REPLICATION_RULE_ENABLED_KEY_DEFAULT);
    if (replicationRuleGenerateEnabled && replicationRuleGenerateKafkaTrigger == null) {
      LOG.info("Start replication rule generate kafka trigger.");
      replicationRuleGenerateKafkaTrigger = new ReplicationRuleGenerateKafkaTrigger(conf);
    }
  }

  private void stopReplicationRuleGenerateKafkaTrigger() {
    LOG.info("Stop replication rule generate kafka trigger.");
    if (replicationRuleGenerateKafkaTrigger != null) {
      replicationRuleGenerateKafkaTrigger.shutdown();
      replicationRuleGenerateKafkaTrigger = null;
    }
  }

  @Override // ReconfigurableBase
  protected String reconfigurePropertyImpl(String property, String newVal) {
    return getConf().get(property);
  }

  @Override // ReconfigurableBase
  public Collection<String> getReconfigurableProperties() {
    return reconfigurableProperties;
  }

  @Override  // ReconfigurableBase
  protected Configuration getNewConf() {
    return new HdfsConfiguration();
  }

  /**
   */
  public static void main(String[] arg) {
    if (DFSUtil.parseHelpArgument(arg, ZoneService.USAGE, System.out, true)) {
      System.exit(0);
    }

    try {
      StringUtils.startupShutdownMessage(ZoneService.class, arg, LOG);
      createZoneService(arg, null);
    } catch (Throwable e) {
      LOG.error("Failed to start zoneservice.", e);
      terminate(1, e);
    }
  }
}