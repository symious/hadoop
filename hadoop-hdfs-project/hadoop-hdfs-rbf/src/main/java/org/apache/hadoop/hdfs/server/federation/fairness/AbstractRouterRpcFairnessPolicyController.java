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

package org.apache.hadoop.hdfs.server.federation.fairness;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamespaceInfo;
import org.apache.hadoop.hdfs.server.federation.router.FederationUtil;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcClient;
import org.apache.hadoop.util.Time;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;

import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_FAIR_HANDLER_COUNT_KEY_PREFIX;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_FAIR_MINIMUM_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_FAIR_MINIMUM_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_WAIT_TIME_FOR_ACQUIRING_PERMIT_DEFAULT;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_WAIT_TIME_FOR_ACQUIRING_PERMIT_KEY;

/**
 * Base fairness policy that implements @RouterRpcFairnessPolicyController.
 * Internally a map of nameservice to Semaphore is used to control permits.
 */
public class AbstractRouterRpcFairnessPolicyController
    implements RouterRpcFairnessPolicyController {

  public static final Logger LOG =
      LoggerFactory.getLogger(AbstractRouterRpcFairnessPolicyController.class);

  public static final String ERROR_MSG = "Configured handlers "
      + DFS_ROUTER_HANDLER_COUNT_KEY + '='
      + " %d is less than the minimum required handlers %d";
  public static final String ERROR_NS_MSG =
      "Configured handlers %s=%d is less than the minimum required handlers %d";
  protected FairnessPolicyControllerMetrics metrics;

  protected volatile int maxWaitingTime = DFS_ROUTER_WAIT_TIME_FOR_ACQUIRING_PERMIT_DEFAULT;
  private static final ThreadLocal<Long> PERMIT_ACQUISITION_TIME = new ThreadLocal<>();

  /** Hash table to hold AbstractNSPermitManager for each name service. */
  private Map<String, AbstractPermitManager> permits = new HashMap<>();
  /** Version to support dynamically change permits. **/
  private final int version;

  AbstractRouterRpcFairnessPolicyController(int version, Configuration conf) {
    this.version = version;
    this.maxWaitingTime = conf.getInt(
        DFS_ROUTER_WAIT_TIME_FOR_ACQUIRING_PERMIT_KEY,
        DFS_ROUTER_WAIT_TIME_FOR_ACQUIRING_PERMIT_DEFAULT);
    this.metrics = FairnessPolicyControllerMetrics.create(this);
  }

  @VisibleForTesting
  public void resetMetrics() {
    this.metrics = FairnessPolicyControllerMetrics.create(this);
  }

  public int getVersion() {
    return version;
  }

  /**
   * Init the permits.
   */
  public void initPermits(Map<String, AbstractPermitManager> newPermits) {
    this.permits = newPermits;
  }

  protected int getMaxWaitingTime() {
    return this.maxWaitingTime;
  }

  @Override
  public Permit acquirePermit(String nsId) {
    LOG.debug("Taking lock for nameservice {}", nsId);
    AbstractPermitManager permitManager = permits.get(nsId);
    if (permitManager != null) {
      long start = Time.monotonicNow();
      PERMIT_ACQUISITION_TIME.set(start);
      Permit result = permitManager.acquirePermit();
      metrics.addPermitWaitTime(Time.monotonicNow() - start, nsId);
      return result;
    } else {
      metrics.incrMissingPermit();
      LOG.warn("Can't find NSPermit for {}.", nsId, new Throwable());
      return Permit.NO_PERMIT;
    }
  }

  @Override
  public void releasePermit(String nsId, Permit permitInstance) {
    if (permitInstance == null || permitInstance.isPermitNotRequired()) {
      return;
    }
    AbstractPermitManager permitManager =
        this.permits.get(nsId);
    if (permitManager != null) {
      permitManager.releasePermit(permitInstance);
      metrics.addPermitHoldTime(Time.monotonicNow() - PERMIT_ACQUISITION_TIME.get(), nsId);
    }
  }

  protected Map<String, AbstractPermitManager> getPermits() {
    return this.permits;
  }

  @Override
  public void shutdown() {
    LOG.debug("Shutting down router fairness policy controller");
    // drain all semaphores
    for (AbstractPermitManager sema: this.permits.values()) {
      sema.drainPermits();
    }
  }

  @Override
  public String getAvailableHandlerOnPerNs() {
    JSONObject json = new JSONObject();
    for (Map.Entry<String, AbstractPermitManager> entry : permits.entrySet()) {
      try {
        String nsId = entry.getKey();
        int availableHandler = entry.getValue().availablePermits();
        json.put(nsId, availableHandler);
      } catch (JSONException e) {
        LOG.warn("Cannot put {} into JSONObject", entry.getKey(), e);
      }
    }
    return json.toString();
  }

  /**
   * Get All NameServices from conf and membershipStore.
   */
  protected Set<String> getAllNameServices(
      RouterRpcClient rpcClient, Configuration conf) {
    Set<String> allConfiguredNS = FederationUtil.getAllConfiguredNS(conf);
    try {
      if (rpcClient != null) {
        ActiveNamenodeResolver resolver = rpcClient.getNamenodeResolver();
        if (resolver != null) {
          Set<FederationNamespaceInfo> federationNamespaceInfos =
              rpcClient.getNamenodeResolver().getNamespaces();
          for (FederationNamespaceInfo nsInfo : federationNamespaceInfos) {
            allConfiguredNS.add(nsInfo.getNameserviceId());
          }
        }
      }
    } catch (IOException ioe) {
      LOG.warn("GetAll NameServices from ZK failed, ", ioe);
    }
    return allConfiguredNS;
  }

  public String getPermitCapacityPerNs() {
    JSONObject json = new JSONObject();
    for (Map.Entry<String, AbstractPermitManager> entry : permits.entrySet()) {
      try {
        json.put(entry.getKey(), entry.getValue().getPermitCap());
      } catch (JSONException e) {
        LOG.warn("Cannot put {} into JSONObject", entry.getKey(), e);
      }
    }
    return json.toString();
  }

  public CompositeData getPermitCapacityPerNsAsJson() {
    if (permits.isEmpty()) {
      return null;
    }

    try {
      int size = permits.size();
      String[] fields = permits.keySet().toArray(new String[0]);
      OpenType[] types =
          Collections.nCopies(size, SimpleType.INTEGER)
              .toArray(new OpenType[0]);
      Integer[] values = new Integer[size];
      for (int i = 0; i < size; i++) {
        values[i] = permits.get(fields[i]).getPermitCap();
      }

      CompositeType type = new CompositeType(this.getClass().getName(),
          this.getClass().getName(), fields, fields, types);
      return new CompositeDataSupport(type, fields, values);
    } catch (OpenDataException e) {
      LOG.warn("Failed to get permit capacity metrics as CompositeData", e);
      return null;
    }
  }

  protected int getDedicatedHandlers(Configuration conf, String nsId) {
    int minimumHandlerPerNs = conf.getInt(DFS_ROUTER_FAIR_MINIMUM_HANDLER_COUNT_KEY,
        DFS_ROUTER_FAIR_MINIMUM_HANDLER_COUNT_DEFAULT);
    int dedicatedHandlers = conf.getInt(
        DFS_ROUTER_FAIR_HANDLER_COUNT_KEY_PREFIX + nsId, 0);
    if (dedicatedHandlers > 0 && dedicatedHandlers < minimumHandlerPerNs) {
      String msg = String.format(ERROR_NS_MSG, DFS_ROUTER_FAIR_HANDLER_COUNT_KEY_PREFIX + nsId,
          dedicatedHandlers, minimumHandlerPerNs);
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    } else if (dedicatedHandlers <= 0) {
      dedicatedHandlers = minimumHandlerPerNs;
    }
    return dedicatedHandlers;
  }

  /**
   * Validate all configured dedicated handlers for the nameservices.
   * @return sum of dedicated handlers of all nameservices
   * @throws IllegalArgumentException
   *         if total dedicated handlers more than handler count.
   */
  protected int validateHandlersCount(Configuration conf,
      int handlerCount, Set<String> allConfiguredNS) {
    int totalDedicatedHandlers = 0;
    for (String nsId : allConfiguredNS) {
      totalDedicatedHandlers += getDedicatedHandlers(conf, nsId);
    }
    if (totalDedicatedHandlers > handlerCount) {
      String msg = String.format(ERROR_MSG, handlerCount,
          totalDedicatedHandlers);
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }
    return totalDedicatedHandlers;
  }
}
