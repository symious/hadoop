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

package org.apache.hadoop.yarn.server.globalpolicygenerator;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.JvmPauseMonitor;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.globalpolicygenerator.applicationcleaner.ApplicationCleaner;
import org.apache.hadoop.yarn.server.globalpolicygenerator.subclustercleaner.SubClusterCleaner;
import org.apache.hadoop.yarn.server.globalpolicygenerator.webapp.GPGWebApp;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWebAppUtil;
import org.apache.hadoop.yarn.server.webapp.WebServiceClient;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * Global Policy Generator (GPG) is a Yarn Federation component. By tuning the
 * Federation policies in Federation State Store, GPG overlooks the entire
 * federated cluster and ensures that the system is tuned and balanced all the
 * time.
 *
 * The GPG operates continuously but out-of-band from all cluster operations,
 * that allows to enforce global invariants, affect load balancing, trigger
 * draining of sub-clusters that will undergo maintenance, etc.
 */
public class GlobalPolicyGenerator extends CompositeService {

  public static final Logger LOG =
      LoggerFactory.getLogger(GlobalPolicyGenerator.class);

  // YARN Variables
  private static CompositeServiceShutdownHook gpgShutdownHook;
  public static final int SHUTDOWN_HOOK_PRIORITY = 30;
  private Configuration conf;
  private AtomicBoolean isStopping = new AtomicBoolean(false);
  private static final String METRICS_NAME = "Global Policy Generator";
  private static long gpgStartupTime = System.currentTimeMillis();

  // Federation Variables
  private GPGContext gpgContext;

  // Scheduler service that runs tasks periodically
  private ScheduledThreadPoolExecutor scheduledExecutorService;
  private SubClusterCleaner subClusterCleaner;
  private ApplicationCleaner applicationCleaner;

  private String manageRouterList;

  private String webAppAddress;
  private JvmPauseMonitor pauseMonitor;
  private WebApp webApp;

  public GlobalPolicyGenerator() {
    super(GlobalPolicyGenerator.class.getName());
    this.gpgContext = new GPGContextImpl();
  }

  @Override
  protected void serviceInit(Configuration config) throws Exception {

    this.conf = config;

    // Set up the context
    this.gpgContext
        .setStateStoreFacade(FederationStateStoreFacade.getInstance());

    this.scheduledExecutorService = new ScheduledThreadPoolExecutor(
        conf.getInt(YarnConfiguration.GPG_SCHEDULED_EXECUTOR_THREADS,
            YarnConfiguration.DEFAULT_GPG_SCHEDULED_EXECUTOR_THREADS));
    this.subClusterCleaner = new SubClusterCleaner(this.conf, this.gpgContext);

    this.applicationCleaner = FederationStateStoreFacade.createInstance(conf,
        YarnConfiguration.GPG_APPCLEANER_CLASS,
        YarnConfiguration.DEFAULT_GPG_APPCLEANER_CLASS,
        ApplicationCleaner.class);
    this.applicationCleaner.init(conf, this.gpgContext);

    this.webAppAddress = WebAppUtils.getGPGWebAppURLWithoutScheme(this.conf);

    this.manageRouterList = this.conf.get(
        YarnConfiguration.GPG_MANAGE_ROUTER_GROUP_LIST,
        YarnConfiguration.DEFAULT_ROUTER_GROUP_LIST);

    DefaultMetricsSystem.initialize(METRICS_NAME);
    JvmMetrics jm = JvmMetrics.initSingleton("GPG", null);
    pauseMonitor = new JvmPauseMonitor();
    addService(pauseMonitor);
    jm.setPauseMonitor(pauseMonitor);

    // Init WebServiceClient
    WebServiceClient.initialize(config);

    // super.serviceInit after all services are added
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();

    // Schedule SubClusterCleaner service
    long scCleanerIntervalMs = getConfig().getLong(
        YarnConfiguration.GPG_SUBCLUSTER_CLEANER_INTERVAL_MS,
        YarnConfiguration.DEFAULT_GPG_SUBCLUSTER_CLEANER_INTERVAL_MS);
    if (scCleanerIntervalMs > 0) {
      this.scheduledExecutorService.scheduleAtFixedRate(this.subClusterCleaner,
          0, scCleanerIntervalMs, TimeUnit.MILLISECONDS);
      LOG.info("Scheduled sub-cluster cleaner with interval: {} ms",
          scCleanerIntervalMs);
    }

    // Schedule ApplicationCleaner service
    long appCleanerIntervalMs =
        getConfig().getLong(YarnConfiguration.GPG_APPCLEANER_INTERVAL_MS,
            YarnConfiguration.DEFAULT_GPG_APPCLEANER_INTERVAL_MS);
    if (appCleanerIntervalMs > 0) {
      this.scheduledExecutorService.scheduleAtFixedRate(this.applicationCleaner,
          0, appCleanerIntervalMs, TimeUnit.MILLISECONDS);
      LOG.info("Scheduled application cleaner with interval: {} ms",
          appCleanerIntervalMs);
    }

    startWepApp();
  }

  @Override
  protected void serviceStop() throws Exception {
    try {
      if (this.scheduledExecutorService != null
          && !this.scheduledExecutorService.isShutdown()) {
        this.scheduledExecutorService.shutdown();
        LOG.info("Stopped ScheduledExecutorService");
      }
    } catch (Exception e) {
      LOG.error("Failed to shutdown ScheduledExecutorService", e);
      throw e;
    }

    if (this.isStopping.getAndSet(true)) {
      return;
    }
    if (webApp != null) {
      webApp.stop();
    }

    DefaultMetricsSystem.shutdown();
    WebServiceClient.destroy();

    super.serviceStop();
  }

  public String getName() {
    return "FederationGlobalPolicyGenerator";
  }

  public GPGContext getGPGContext() {
    return this.gpgContext;
  }

  private void initAndStart(Configuration conf, boolean hasToReboot) {
    // Remove the old hook if we are rebooting.
    if (hasToReboot && null != gpgShutdownHook) {
      ShutdownHookManager.get().removeShutdownHook(gpgShutdownHook);
    }

    gpgShutdownHook = new CompositeServiceShutdownHook(this);
    ShutdownHookManager.get().addShutdownHook(gpgShutdownHook,
        SHUTDOWN_HOOK_PRIORITY);

    this.init(conf);
    this.start();
  }

  @VisibleForTesting
  public void startWepApp() {
    LOG.info("Instantiating GPGWebApp at " + webAppAddress);
    RMWebAppUtil.setupSecurityAndFilters(conf, null);
    WebApps.Builder<Object> builder =
        WebApps.$for("cluster", null, null, "ws").
            with(conf).at(webAppAddress);
    webApp = builder.start(new GPGWebApp(this));
  }

  @SuppressWarnings("resource")
  public static void startGPG(String[] argv, Configuration conf) {
    boolean federationEnabled =
        conf.getBoolean(YarnConfiguration.FEDERATION_ENABLED,
            YarnConfiguration.DEFAULT_FEDERATION_ENABLED);
    try {
      if (federationEnabled) {
        Thread.setDefaultUncaughtExceptionHandler(
            new YarnUncaughtExceptionHandler());
        StringUtils.startupShutdownMessage(GlobalPolicyGenerator.class, argv,
            LOG);
        GlobalPolicyGenerator globalPolicyGenerator =
            new GlobalPolicyGenerator();
        globalPolicyGenerator.initAndStart(conf, false);
      } else {
        LOG.warn("Federation is not enabled. The gpg cannot start.");
      }
    } catch (Throwable t) {
      LOG.error("Error starting globalpolicygenerator", t);
      System.exit(-1);
    }
  }

  public static long getGPGStartupTime() {
    return gpgStartupTime;
  }

  public String getManageRouterList(){
    return manageRouterList;
  }

  public String getMachineListByRouterGroup(String group){
    return this.conf.get(
        YarnConfiguration.GPG_MANAGE_SPECIAL_ROUTER_GROUP_MACHINES_PREFIX
            + group, YarnConfiguration.DEFAULT_ROUTER_GROUP_MACHINE_LIST);
  }

  public static void main(String[] argv) {
    startGPG(argv, new YarnConfiguration());
  }
}
