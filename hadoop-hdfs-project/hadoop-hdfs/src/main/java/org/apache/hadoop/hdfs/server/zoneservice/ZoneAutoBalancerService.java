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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.balancer.ExitStatus;
import org.apache.hadoop.hdfs.server.balancer.NameNodeConnector;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress;
import org.apache.hadoop.hdfs.server.zoneservice.metrics.ZoneAutoBalancerMetrics;
import org.apache.hadoop.hdfs.server.zoneservice.utils.RunMode;
import org.apache.hadoop.hdfs.server.zoneservice.utils.ZoneServiceUtil;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.tracing.TraceUtils;
import org.apache.hadoop.tracing.TracerConfigurationManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.htrace.core.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ZONESERVICE_AUTO_BALANCER_HTTP_ADDRESS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ZONESERVICE_AUTO_BALANCER_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ZONESERVICE_AUTO_BALANCER_HTTP_BIND_HOST_KEY;
import static org.apache.hadoop.util.ExitUtil.terminate;

public class ZoneAutoBalancerService {
  public static final Logger LOG = LoggerFactory.getLogger(ZoneAutoBalancerService.class);
  ZoneAutoBalancerMetrics metrics = ZoneAutoBalancerMetrics.create();

  private static final String AUTOBALANCER_HTRACE_PREFIX = "autobalancer.htrace.";
  protected final Tracer tracer;
  protected final TracerConfigurationManager tracerConfigurationManager;
  protected ZoneServiceHttpServer httpServer;
  private static final StartupProgress startupProgress = new StartupProgress();

  // key: namespace; value: message get from kafka
  // message will contain partition, offset, file name, file id
  private final Map<String, ArrayBlockingQueue<MoverPathInfo>> pathQueue;
  // key: kafka partition name; value: partition offset
  private final Map<Integer, List<Long>> offsetMap;
  // key: namespace; value: ZoneMover on this namespace
  private final Map<String, Thread> zmThreadMap;

  private final ZoneAutoBalancerTrigger zoneAutoBalancerTrigger;

  private final ExecutorService executorService;
  private final Thread triggerThread;
  private final Thread distributor;

  public ZoneAutoBalancerService(Configuration conf) throws IOException {
    this.tracer = new Tracer.Builder("ZoneServiceAutoBalancer").
        conf(TraceUtils.wrapHadoopConf(AUTOBALANCER_HTRACE_PREFIX, conf)).
        build();
    this.tracerConfigurationManager =
        new TracerConfigurationManager(AUTOBALANCER_HTRACE_PREFIX, conf);

    int executorConstraint = conf.getInt(
        DFSConfigKeys.DFS_ZONESERVICE_AUTO_BALANCER_EXECUTOR_THREAD_CONSTRAINT_KEY,
        DFSConfigKeys.DFS_ZONESERVICE_AUTO_BALANCER_EXECUTOR_THREAD_CONSTRAINT_DEFAULT);

    pathQueue = new ConcurrentHashMap<>();
    offsetMap = new ConcurrentHashMap<>();
    zmThreadMap = new ConcurrentHashMap<>();
    executorService = new ThreadPoolExecutor(executorConstraint, executorConstraint, 0L,
        TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1));
    zoneAutoBalancerTrigger = new ZoneAutoBalancerTrigger(conf);

    // Start trigger to process hdfs audit log in Kafka
    triggerThread = new Thread(
        () -> zoneAutoBalancerTrigger.monitorPaths(pathQueue, offsetMap));
    // Thread to move the file chosen by trigger
    distributor = new Distributor();
    triggerThread.start();
    distributor.start();

    startHttpServer(conf);
  }

  public static void createService(String[] argv, Configuration conf)
      throws IOException {
    LOG.info("createZoneServiceAutoBalancer " + Arrays.asList(argv));
    if (conf == null)
      conf = new HdfsConfiguration();

    DefaultMetricsSystem.initialize("ZoneAutoBalancer");
    new ZoneAutoBalancerService(conf);
  }

  private void startHttpServer(final Configuration conf) throws IOException {
    httpServer = new ZoneAutoBalancerHttpServer(conf, getHttpServerBindAddress(conf));
    httpServer.start();
    httpServer.setStartupProgress(startupProgress);
  }

  protected InetSocketAddress getHttpServerBindAddress(Configuration conf) {
    InetSocketAddress bindAddress = getHttpAddress(conf);

    final String bindHost = conf.getTrimmed(DFS_ZONESERVICE_AUTO_BALANCER_HTTP_BIND_HOST_KEY);
    if (bindHost != null && !bindHost.isEmpty()) {
      bindAddress = new InetSocketAddress(bindHost, bindAddress.getPort());
    }

    return bindAddress;
  }

  /** @return the ZoneService AutoBalancer HTTP address. */
  public static InetSocketAddress getHttpAddress(Configuration conf) {
    return  NetUtils.createSocketAddr(
        conf.getTrimmed(DFS_ZONESERVICE_AUTO_BALANCER_HTTP_ADDRESS_KEY,
            DFS_ZONESERVICE_AUTO_BALANCER_HTTP_ADDRESS_DEFAULT));
  }

  /**
   * Thread to create ZoneMover for every namespace and move the file
   */
  class Distributor extends Thread {
    public Distributor() {
      super("Distributor");
    }

    @Override
    public void run() {
      LOG.info("Distributor thread have been started!");
      final Configuration conf = new Configuration();
      while (true) {
        try {
          for (final String ns : pathQueue.keySet()) {
            if (!zmThreadMap.containsKey(ns) || !zmThreadMap.get(ns).isAlive()) {
              LOG.info("Create ZoneMover thread for " + ns);
              MoverThread moverThread = new MoverThread(ns, conf,
                  executorService, pathQueue.get(ns), offsetMap);
              moverThread.setName("MoverThread for " + ns);
              zmThreadMap.put(ns, moverThread);
              moverThread.start();
            }
          }
          metrics.setActiveZoneMoverCount(zmThreadMap.size());
          metrics.setOffsetSize(offsetMap);
          metrics.setOffset(offsetMap);
          metrics.setPathQueueSize(pathQueue);
          Thread.sleep(1000);
        } catch (Throwable e) {
          LOG.error("Distributor failed, ", e);
          break;
        }
      }
    }
  }

  static class MoverThread extends Thread {
    private final String ns;
    private final Configuration conf;
    private final ExecutorService executorService;
    private final ArrayBlockingQueue<MoverPathInfo> nsPathQueue;
    private final Map<Integer, List<Long>> offsetMap;

    public MoverThread(String ns, Configuration conf, ExecutorService executorService,
        ArrayBlockingQueue<MoverPathInfo> nsPathQueue, Map<Integer, List<Long>> offsetMap) {
      this.ns = ns;
      this.conf = conf;
      this.executorService = executorService;
      this.nsPathQueue = nsPathQueue;
      this.offsetMap = offsetMap;
    }

    @Override
    public void run() {
      final URI namenode = ZoneServiceUtil.getNamespaceUri(ns, conf);
      LOG.info("String ont thread to move file in {}.", namenode.getAuthority());
      final ZoneMover zs;
      try {
        NameNodeConnector nnc = new NameNodeConnector(ZoneMover.class.getSimpleName(),
            namenode, ZoneMover.getIdPath(RunMode.MONITOR),
            new ArrayList<Path>(), conf, 1);
        nnc.getKeyManager().startBlockKeyUpdater();
        zs = new ZoneMover(nnc, conf, new ReplicationRule(), new AtomicInteger(0));
        zs.init(conf);
      } catch (Throwable e) {
        LOG.error("Initialize the ZoneMover failed!" , e);
        return;
      }

      while (true) {
        try {
          MoverPathInfo message = this.nsPathQueue.take();
          final Date startTime = new Date();
          int partition = message.getPartition();
          long offset = message.getRecordOffset();

          offsetMap.get(partition).remove(offset);
          final String path = message.getFullPath();
          final long fileId = message.getFileId();
          LOG.info("Processing the path --- {} with fileId {}.", path, fileId);

          executorService.execute(() -> {
            ExitStatus exitStatus = zs.run(path, fileId);

            AuditLogger.logRuleProcess("autobalancer", namenode.getAuthority(),
                path, String.valueOf(fileId), startTime, new Date(),
                ZoneServiceUtil.convertExitStatus2ResultCode(exitStatus).getMsg(), "Batch");
          });
        } catch (Throwable e) {
          LOG.error("MoverThread of {} failed, ", ns, e);
          break;
        }
      }
    }
  }

  public static void main(String[] arg) {
    if (DFSUtil.parseHelpArgument(arg,"", System.out, true)) {
      System.exit(0);
    }

    try {
      StringUtils.startupShutdownMessage(ZoneAutoBalancerService.class, arg, LOG);
      createService(arg, null);
    } catch (Throwable e) {
      LOG.error("Failed to start zone auto balancer.", e);
      terminate(1, e);
    }
  }
}