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

import com.sun.jersey.spi.resource.Singleton;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.balancer.ExitStatus;
import org.apache.hadoop.hdfs.server.namenode.AuditLogger;
import org.apache.hadoop.hdfs.server.zoneservice.ReplicationRule;
import org.apache.hadoop.hdfs.server.zoneservice.ZoneChecker;
import org.apache.hadoop.hdfs.server.zoneservice.ZoneMover;
import org.apache.hadoop.hdfs.server.zoneservice.ZoneMoverKafkaTrigger;
import org.apache.hadoop.hdfs.server.zoneservice.ZoneMoverTrigger;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Semaphore;

@Path("replicarule/")
@Singleton
public class ZoneServiceRestAPI {
  private static final String defaultRatio = "-1";
  private static final String defaultMode = "batch";
  private static final String defaultNull = "N/A";
  private static final Configuration conf = new Configuration();
  private static final int maxThread = conf.getInt(
      DFSConfigKeys.DFS_ZONESERVICE_THREADS_KEY,
      DFSConfigKeys.DFS_ZONESERVICE_THREADS_DEFAULT);
  private static final Semaphore semaphore = new Semaphore(maxThread);
  public ZoneServiceRestAPI() { }

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
    StackTraceElement[] elements = Thread.currentThread().getStackTrace();
    StringBuilder result = new StringBuilder();
    for (StackTraceElement element : elements)
      result.append("File: ").append(element.getFileName()).append(" Line: ")
          .append(element.getLineNumber()).append(" Method: ")
          .append(element.getMethodName()).append("\n");
    AuditLogger.logRuleProcess(
        Thread.currentThread().getStackTrace()[1].getMethodName(), defaultNull,
        defaultNull, defaultNull, startTime, new Date(),
        ResultCode.SUCCESS.getMsg(), defaultNull);
    return result.toString();
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
    if (semaphore.availablePermits() == 0) {
      AuditLogger.logRuleProcess(
          Thread.currentThread().getStackTrace()[1].getMethodName(), nameSpace,
          path, defaultNull, startTime, new Date(),
          ResultCode.THREAD_FULL.getMsg(), defaultNull);
      return ResultCode.THREAD_FULL.toString();
    }
    try {
      semaphore.acquire();
      Map<ReplicationRule, Set<String>> hashMap =
          checkPath(nameSpace, path, ratio);
      semaphore.release();
      AuditLogger.logRuleProcess(
          Thread.currentThread().getStackTrace()[1].getMethodName(), nameSpace,
          path, defaultNull, startTime, new Date(),
          ResultCode.SUCCESS.getMsg(), defaultNull);
      return hashMap.toString();
    } catch (InterruptedException e) {
      e.printStackTrace();
      AuditLogger.logRuleProcess(
          Thread.currentThread().getStackTrace()[1].getMethodName(), nameSpace,
          path, defaultNull, startTime, new Date(),
          ResultCode.INTERRUPTED.getMsg(), defaultNull);
      return ResultCode.INTERRUPTED.toString();
    }
  }

  /**
   * refresh replication rule, not allow use monitor mode by this method
   * @param hsr          http servlet request
   * @param nameSpace    URI of the NameNode
   * @param replicaRule  the replica rule to apply
   * @param mode         mode to run zonemover
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
      @QueryParam("mode") @DefaultValue(defaultMode) String mode,
      @PathParam("path") String path) {
    Date startTime = new Date();
    if (semaphore.availablePermits() == 0) {
      AuditLogger.logRuleProcess(
          Thread.currentThread().getStackTrace()[1].getMethodName(), nameSpace,
          path, replicaRule, startTime, new Date(),
          ResultCode.THREAD_FULL.getMsg(), mode);
      return ResultCode.THREAD_FULL.toString();
    }
    try {
      semaphore.acquire();
      ResultCode result = movePath(nameSpace, path, replicaRule, mode);
      semaphore.release();
      AuditLogger.logRuleProcess(
          Thread.currentThread().getStackTrace()[1].getMethodName(), nameSpace,
          path, replicaRule, startTime, new Date(), result.getMsg(), mode);
      return result.toString();
    } catch (InterruptedException e) {
      e.printStackTrace();
      AuditLogger.logRuleProcess(
          Thread.currentThread().getStackTrace()[1].getMethodName(), nameSpace,
          path, replicaRule, startTime, new Date(),
          ResultCode.INTERRUPTED.getMsg(), mode);
      return ResultCode.INTERRUPTED.toString();
    }
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
    Date startTime = new Date();
    if (semaphore.availablePermits() == 0) {
      AuditLogger.logRuleProcess(
          Thread.currentThread().getStackTrace()[1].getMethodName(), nameSpace,
          path, replicaRule, startTime, new Date(),
          ResultCode.THREAD_FULL.getMsg(), defaultMode);
      return ResultCode.THREAD_FULL.toString();
    }
    try {
      semaphore.acquire();
      ResultCode result =
          movePath(nameSpace, path, replicaRule, defaultMode);
      semaphore.release();
      AuditLogger.logRuleProcess(
          Thread.currentThread().getStackTrace()[1].getMethodName(), nameSpace,
          path, replicaRule, startTime, new Date(),
          result.getMsg(), defaultMode);
      return result.toString();
    } catch (InterruptedException e) {
      e.printStackTrace();
      AuditLogger.logRuleProcess(
          Thread.currentThread().getStackTrace()[1].getMethodName(), nameSpace,
          path, replicaRule, startTime, new Date(),
          ResultCode.INTERRUPTED.getMsg(), defaultMode);
      return ResultCode.INTERRUPTED.toString();
    }
  }

  /**
   * Move the path with ZoneMover
   * @param nameSpace   URI of the NameNode
   * @param path        the path to apply the rule
   * @param replicaRule the replica rule to apply
   * @param mode        if use monitor mode or not
   * @return ResultCode
   */
  protected ResultCode movePath(String nameSpace, String path, String replicaRule,
                                String mode) {
    final Configuration conf = new Configuration();
    final URI namenode = getNamespaceUri(nameSpace, conf);
    final ReplicationRule replicationRule =
        ReplicationRule.parseFromString(replicaRule);
    final List<org.apache.hadoop.fs.Path> paths = new ArrayList<>();
    paths.add(new org.apache.hadoop.fs.Path(path));

    try {
      if (mode.equals("batch")) {
        switch (Objects.requireNonNull(ExitStatus.getExitStatusByCode(
            ZoneMover.run(conf, namenode, paths, replicationRule)))) {
        case SUCCESS:
          return ResultCode.SUCCESS;
        case IN_PROGRESS:
          return ResultCode.IN_PROGRESS;
        case ALREADY_RUNNING:
          return ResultCode.ALREADY_RUNNING;
        case NO_MOVE_BLOCK:
          return ResultCode.NO_MOVE_BLOCK;
        case NO_MOVE_PROGRESS:
          return ResultCode.NO_MOVE_PROGRESS;
        case IO_EXCEPTION:
          return ResultCode.IO_EXCEPTION;
        case ILLEGAL_ARGUMENTS:
          return ResultCode.ILLEGAL_ARGUMENTS;
        case INTERRUPTED:
          return ResultCode.INTERRUPTED;
        case UNFINALIZED_UPGRADE:
          return ResultCode.UNFINALIZED_UPGRADE;
        }
      } else {
        final ZoneMoverTrigger zoneMoverTrigger =
            new ZoneMoverKafkaTrigger(conf, paths);
        //if in monitor mode, there will not be any return
        //TODO: monitor mode will start with the zone service
        //TODO: monitor mode will be controlled by paths and rules in a storage
        ZoneMover.run(zoneMoverTrigger, conf, namenode, paths, replicationRule);
        return ResultCode.MONITOR_MODE_ON;
      }
      return ResultCode.UNKNOWNERROR;
    } catch (IOException e) {
      e.printStackTrace();
      return ResultCode.IO_EXCEPTION;
    } catch (InterruptedException e) {
      e.printStackTrace();
      return ResultCode.INTERRUPTED;
    }
  }

  /**
   * check path with zone checker
   * @param nameSpace URI of the NameNode
   * @param path      the path to be checked
   * @param ratio     the ratio of path will be checked
   * @return check result: replication-path map
   */
  protected Map<ReplicationRule, Set<String>> checkPath(String nameSpace,
                                                        String path, String ratio) {
    Configuration conf = new Configuration();
    URI namenode = getNamespaceUri(nameSpace, conf);
    return ZoneChecker.getReplicaRule(conf, namenode, path,
        Float.parseFloat(ratio));
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
}