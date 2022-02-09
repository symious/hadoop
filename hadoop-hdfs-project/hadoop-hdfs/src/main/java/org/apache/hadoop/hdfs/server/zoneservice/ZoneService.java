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

import com.google.common.collect.Sets;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.ReconfigurableBase;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgressMetrics;
import org.apache.hadoop.hdfs.server.zoneservice.store.StoreDriver;
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
import java.util.Arrays;
import java.util.Collection;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_USER_GROUP_METRICS_PERCENTILES_INTERVALS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_METRICS_PERCENTILES_INTERVALS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ZONESERVICE_HTTP_BIND_HOST_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ZONESERVICE_HTTP_ADDRESS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ZONESERVICE_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ZONESERVICE_STORE_DRIVER_CLASS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ZONESERVICE_STORE_DRIVER_CLASS_DEFAULT;
import static org.apache.hadoop.util.ExitUtil.terminate;

public class ZoneService extends ReconfigurableBase  {
  public static final Logger LOG =
      LoggerFactory.getLogger(ZoneService.class);

  // A list of property that are reconfigurable at runtime.
  private final TreeSet<String> reconfigurableProperties = Sets
      .newTreeSet();

  private static final String USAGE = "Usage: ";

  // httpServer
  protected ZoneServiceHttpServer httpServer;

  // store driver
  protected StoreDriver driver;

  private static final String ZONESERVICE_HTRACE_PREFIX = "zoneservice.htrace.";

  private static final StartupProgress startupProgress = new StartupProgress();

  protected final Tracer tracer;
  protected final TracerConfigurationManager tracerConfigurationManager;

  public ZoneService(Configuration conf) throws IOException {
    this.tracer = new Tracer.Builder("ZoneService").
        conf(TraceUtils.wrapHadoopConf(ZONESERVICE_HTRACE_PREFIX, conf)).
        build();
    this.tracerConfigurationManager =
        new TracerConfigurationManager(ZONESERVICE_HTRACE_PREFIX, conf);

    try {
      initialize(getConf());
    } catch (IOException | HadoopIllegalArgumentException e) {
      this.stopAtException(e);
      throw e;
    }
    AtomicBoolean started = new AtomicBoolean(false);
    started.set(true);
  }

  public static ZoneService createZoneService(String[] argv, Configuration conf)
      throws IOException {
    LOG.info("createZoneService " + Arrays.asList(argv));
    if (conf == null)
      conf = new HdfsConfiguration();

    DefaultMetricsSystem.initialize("ZoneService");
    return new ZoneService(conf);
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
        conf.getTrimmed(DFS_ZONESERVICE_HTTP_ADDRESS_KEY, DFS_ZONESERVICE_HTTP_ADDRESS_DEFAULT));
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
    stopHttpServer();
    tracer.close();
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
