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
package org.apache.hadoop.hdfs.server.zoneservice.web.resources;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import com.google.inject.Singleton;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.zoneservice.AuditLogger;
import org.apache.hadoop.hdfs.server.zoneservice.ReplicationRule;
import org.apache.hadoop.hdfs.server.zoneservice.ReplicationRuleManager;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;

@Path("replicarule/")
@Singleton
public class ZoneServiceWebRuleManagementMethods {
  private static final Logger LOG =
      LoggerFactory.getLogger(ZoneMoverWebMigrationRecordMethods.class);
  private static final String defaultRatio = "-1";
  private static final String defaultNull = "N/A";
  private final Semaphore semaphore;
  private final ReplicationRuleManager replicationRuleManager;

  public ZoneServiceWebRuleManagementMethods() {
    Configuration conf = new Configuration();
    int maxThread = conf.getInt(
        DFSConfigKeys.DFS_ZONESERVICE_THREADS_KEY,
        DFSConfigKeys.DFS_ZONESERVICE_THREADS_DEFAULT);
    semaphore = new Semaphore(maxThread);

    replicationRuleManager = new ReplicationRuleManager(conf);
  }

  /**
   * Check active threads for debug
   * @param hsr http servlet request
   * @return active threads in the zone service
   */
  @GET
  @Path("thread/")
  @Consumes()
  @Produces()
  public String getThread(@Context HttpServletRequest hsr) {
    Date startTime = new Date();
    StringBuilder result = new StringBuilder();
    Thread[] ts = new Thread[Thread.activeCount()];
    Thread.enumerate(ts);
    for (Thread t : ts)
      result.append("Name: ").append(t.getName()).append(" ID: ")
          .append(t.getId()).append(" Method: ")
          .append(Arrays.toString(t.getStackTrace())).append("\n");
    AuditLogger.logRuleProcess(
        Thread.currentThread().getStackTrace()[1].getMethodName(), defaultNull,
        defaultNull, defaultNull, startTime, new Date(),
        ResultCode.SUCCESS.getMsg(), defaultNull);
    return new ZoneServiceHttpResponse(result.toString()).toString();
  }

  /**
   * Summary blocks of the given path by data center
   * @param hsr       http servlet request
   * @param nameSpace URI of the NameNode
   * @param path      the path to be checked
   * @return block summary of the given path
   */
  @GET
  @Path("blocksummary/{path:.*}")
  @Consumes()
  @Produces()
  public String blockSummary(@Context HttpServletRequest hsr,
      @QueryParam("namespace") String nameSpace,
      @PathParam("path") String path) {
    Date startTime = new Date();
    //Check if there is any available thread
    if (semaphore.availablePermits() == 0) {
      AuditLogger.logRuleProcess(
          Thread.currentThread().getStackTrace()[1].getMethodName(), nameSpace,
          path, defaultNull, startTime, new Date(),
          ResultCode.THREAD_FULL.getMsg(), defaultNull);
      return new ZoneServiceHttpResponse(ResultCode.THREAD_FULL).toString();
    }

    try {
      semaphore.acquire();
      Map<String, List<Long>> hashMap =
          replicationRuleManager.summaryBlocks(nameSpace, path);
      semaphore.release();
      AuditLogger.logRuleProcess(
          Thread.currentThread().getStackTrace()[1].getMethodName(), nameSpace,
          path, defaultNull, startTime, new Date(),
          ResultCode.SUCCESS.getMsg(), defaultNull);
      if (hashMap.isEmpty()) {
        AuditLogger.logRuleProcess(
            Thread.currentThread().getStackTrace()[1].getMethodName(), nameSpace,
            path, defaultNull, startTime, new Date(),
            ResultCode.NO_DISTRIBUTION.getMsg(), defaultNull);
        return
            new ZoneServiceHttpResponse(ResultCode.NO_DISTRIBUTION).toString();
      }
      JSONObject json = new JSONObject(hashMap);
      return new ZoneServiceHttpResponse(json.toString()).toString();
    } catch (InterruptedException e) {
      LOG.error("Block summary process is interrupted!", e);
      AuditLogger.logRuleProcess(
          Thread.currentThread().getStackTrace()[1].getMethodName(), nameSpace,
          path, defaultNull, startTime, new Date(),
          ResultCode.INTERRUPTED.getMsg(), defaultNull);
      return new ZoneServiceHttpResponse(ResultCode.INTERRUPTED).toString();
    }
  }

  /**
   * Summary blocks of the given path by distribution
   * @param hsr       http servlet request
   * @param nameSpace URI of the NameNode
   * @param path      the path to be checked
   * @return the summary of the given path
   */
  @GET
  @Path("count/{path:.*}")
  @Consumes()
  @Produces()
  public String countSummary(@Context HttpServletRequest hsr,
      @QueryParam("namespace") String nameSpace,
      @PathParam("path") String path) {
    Date startTime = new Date();
    //Check if there is any available thread
    if (semaphore.availablePermits() == 0) {
      AuditLogger.logRuleProcess(
          Thread.currentThread().getStackTrace()[1].getMethodName(), nameSpace,
          path, defaultNull, startTime, new Date(),
          ResultCode.THREAD_FULL.getMsg(), defaultNull);
      return new ZoneServiceHttpResponse(ResultCode.THREAD_FULL).toString();
    }

    try {
      semaphore.acquire();
      Map<String, List<Long>> hashMap =
          replicationRuleManager.countBlocksByDistribution(nameSpace, path);
      semaphore.release();
      AuditLogger.logRuleProcess(
          Thread.currentThread().getStackTrace()[1].getMethodName(), nameSpace,
          path, defaultNull, startTime, new Date(),
          ResultCode.SUCCESS.getMsg(), defaultNull);
      // No result is return
      if (hashMap.isEmpty()) {
        AuditLogger.logRuleProcess(
            Thread.currentThread().getStackTrace()[1].getMethodName(), nameSpace,
            path, defaultNull, startTime, new Date(),
            ResultCode.NO_DISTRIBUTION.getMsg(), defaultNull);
        return
            new ZoneServiceHttpResponse(ResultCode.NO_DISTRIBUTION).toString();
      }
      JSONObject json = new JSONObject(hashMap);
      return new ZoneServiceHttpResponse(json.toString()).toString();
    } catch (InterruptedException e) {
      LOG.error("Count summary process is interrupted!", e);
      AuditLogger.logRuleProcess(
          Thread.currentThread().getStackTrace()[1].getMethodName(), nameSpace,
          path, defaultNull, startTime, new Date(),
          ResultCode.INTERRUPTED.getMsg(), defaultNull);
      return new ZoneServiceHttpResponse(ResultCode.INTERRUPTED).toString();
    }
  }

  /**
   * Use ZoneChecker to get the replication factor of the given path
   * @param hsr       http servlet request
   * @param nameSpace URI of the NameNode
   * @param ratio     the ratio of files need to be checked
   * @param path      the path to be checked
   * @return the replication factor of the given path
   */
  @GET
  @Path("{path:.*}")
  @Consumes()
  @Produces()
  public String getReplicaRule(@Context HttpServletRequest hsr,
      @QueryParam("namespace") String nameSpace,
      @QueryParam("ratio") @DefaultValue(defaultRatio) String ratio,
      @PathParam("path") String path) {
    Date startTime = new Date();
    //Check if there is any available thread
    if (semaphore.availablePermits() == 0) {
      AuditLogger.logRuleProcess(
          Thread.currentThread().getStackTrace()[1].getMethodName(), nameSpace,
          path, defaultNull, startTime, new Date(),
          ResultCode.THREAD_FULL.getMsg(), defaultNull);
      return new ZoneServiceHttpResponse(ResultCode.THREAD_FULL).toString();
    }

    try {
      semaphore.acquire();
      Map<ReplicationRule, Set<String>> hashMap =
          replicationRuleManager.checkPath(nameSpace, path, ratio);
      semaphore.release();
      AuditLogger.logRuleProcess(
          Thread.currentThread().getStackTrace()[1].getMethodName(), nameSpace,
          path, defaultNull, startTime, new Date(),
          ResultCode.SUCCESS.getMsg(), defaultNull);
      if (hashMap.isEmpty()) {
        AuditLogger.logRuleProcess(
            Thread.currentThread().getStackTrace()[1].getMethodName(), nameSpace,
            path, defaultNull, startTime, new Date(),
            ResultCode.NO_DISTRIBUTION.getMsg(), defaultNull);
        return
            new ZoneServiceHttpResponse(ResultCode.NO_DISTRIBUTION).toString();
      }
      JSONObject json = new JSONObject(hashMap);
      return new ZoneServiceHttpResponse(json.toString()).toString();
    } catch (InterruptedException e) {
      LOG.error("Get replica distribution process is interrupted!", e);
      AuditLogger.logRuleProcess(
          Thread.currentThread().getStackTrace()[1].getMethodName(), nameSpace,
          path, defaultNull, startTime, new Date(),
          ResultCode.INTERRUPTED.getMsg(), defaultNull);
      return new ZoneServiceHttpResponse(ResultCode.INTERRUPTED).toString();
    }
  }

  /**
   * set replication rule
   * @param hsr          http servlet request
   * @param nameSpace    URI of the NameNode
   * @param replicaRule  the replica rule to apply
   * @param path         the path to apply the rule
   * @return status of result
   */
  @POST
  @Path("{path:.*}")
  @Consumes()
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public String setReplicaRule(@Context HttpServletRequest hsr,
      @QueryParam("namespace") String nameSpace,
      @QueryParam("rule") String replicaRule,
      @PathParam("path") String path) {
    return new ZoneServiceHttpResponse(
        replicationRuleManager.setBatchProcess(nameSpace, path, replicaRule)).toString();
  }

  /**
   * refresh replication rule, not allow use monitor mode by this method
   * @param hsr          http servlet request
   * @param nameSpace    URI of the NameNode
   * @param replicaRule  the replica rule to apply
   * @param path         the path to apply the rule
   * @return status of result
   */
  @PUT
  @Path("{path:.*}")
  @Consumes()
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public String refreshReplicaRule(@Context HttpServletRequest hsr,
      @QueryParam("namespace") String nameSpace,
      @QueryParam("rule") String replicaRule,
      @PathParam("path") String path) {
    return new ZoneServiceHttpResponse(
        replicationRuleManager.setBatchProcess(nameSpace, path, replicaRule)).toString();
  }

  /**
   * set path-rule mapping
   * @param hsr          http servlet request
   * @param nameSpace    URI of the NameNode
   * @param replicaRule  the replica rule will set on the path
   * @param path         the path to apply the rule
   * @return status of result
   */
  @POST
  @Path("rulemap/{path:.*}")
  @Consumes()
  public String setPathRuleMap(@Context HttpServletRequest hsr,
      @QueryParam("namespace") String nameSpace,
      @QueryParam("rule") String replicaRule,
      @PathParam("path") String path) {
    return new ZoneServiceHttpResponse(replicationRuleManager.createUpdateMap(
        nameSpace, path, replicaRule, true)).toString();
  }

  /**
   * refresh path-rule mapping, not allow create new mapping by this method
   * @param hsr          http servlet request
   * @param nameSpace    URI of the NameNode
   * @param replicaRule  the replica rule will set on the path
   * @param path         the path to apply the rule
   * @return status of result
   */
  @PUT
  @Path("rulemap/{path:.*}")
  @Consumes()
  public String refreshPathRuleMap(@Context HttpServletRequest hsr,
      @QueryParam("namespace") String nameSpace,
      @QueryParam("rule") String replicaRule,
      @PathParam("path") String path) {
    return new ZoneServiceHttpResponse(replicationRuleManager.createUpdateMap(
        nameSpace, path, replicaRule, false)).toString();
  }

  /**
   * Delete path-rule map from zookeeper
   * @param nameSpace the URI of namenode
   * @param path      delete record according to the given path
   * @return status of result
   */
  @DELETE
  @Path("rulemap/{path:.*}")
  public String removePathRuleMap(@Context HttpServletRequest hsr,
      @QueryParam("namespace") String nameSpace,
      @PathParam("path") String path) {
    return new ZoneServiceHttpResponse(replicationRuleManager.removePathRulePair(
        nameSpace, path)).toString();
  }
}
