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

import com.sun.jersey.spi.resource.Singleton;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.balancer.ExitStatus;
import org.apache.hadoop.hdfs.server.zoneservice.AuditLogger;
import org.apache.hadoop.hdfs.server.zoneservice.MonitorThread;
import org.apache.hadoop.hdfs.server.zoneservice.ReplicationRule;
import org.apache.hadoop.hdfs.server.zoneservice.ZoneChecker;
import org.apache.hadoop.hdfs.server.zoneservice.ZoneMover;
import org.apache.hadoop.hdfs.server.zoneservice.ZoneMoverKafkaTrigger;
import org.apache.hadoop.hdfs.server.zoneservice.ZoneMoverTrigger;
import org.apache.hadoop.hdfs.server.zoneservice.store.BaseRecord;
import org.apache.hadoop.hdfs.server.zoneservice.store.MigrationRecord;
import org.apache.hadoop.hdfs.server.zoneservice.store.Query;
import org.apache.hadoop.hdfs.server.zoneservice.store.SignalRecord;
import org.apache.hadoop.hdfs.server.zoneservice.store.StoreDriver;
import org.apache.hadoop.util.ReflectionUtils;
import org.json.JSONObject;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Semaphore;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ZONESERVICE_STORE_DRIVER_CLASS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ZONESERVICE_STORE_DRIVER_CLASS_DEFAULT;

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
  public Class<? extends StoreDriver> driverClass = conf.getClass(
      DFS_ZONESERVICE_STORE_DRIVER_CLASS,
      DFS_ZONESERVICE_STORE_DRIVER_CLASS_DEFAULT,
      StoreDriver.class);
  private final StoreDriver driver =
      ReflectionUtils.newInstance(driverClass, conf);

  public ZoneServiceRestAPI() {
    driver.init(conf, "ReplicationRuleServlet");
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
    for (Thread t : ts) {
      result.append("Name: ").append(t.getName()).append(" ID: ")
          .append(t.getId()).append(" Method: ")
          .append(Arrays.toString(t.getStackTrace())).append("\n");
    }
    AuditLogger.logRuleProcess(
        Thread.currentThread().getStackTrace()[1].getMethodName(), defaultNull,
        defaultNull, defaultNull, startTime, new Date(),
        ResultCode.SUCCESS.getMsg(), defaultNull);
    return new ZoneServiceHttpResponse(result.toString()).toString();
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
      return new ZoneServiceHttpResponse(ResultCode.THREAD_FULL).toString();
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
      e.printStackTrace();
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
    Date startTime = new Date();
    String currentMethod =
        Thread.currentThread().getStackTrace()[1].getMethodName();
    if (semaphore.availablePermits() == 0) {
      AuditLogger.logRuleProcess(currentMethod, nameSpace,
          path, replicaRule, startTime, new Date(),
          ResultCode.THREAD_FULL.getMsg(), defaultMode);
      return new ZoneServiceHttpResponse(ResultCode.THREAD_FULL).toString();
    }
    try {
      semaphore.acquire();
      MigrationRecord migrationRecord =
          new MigrationRecord(nameSpace, path, replicaRule);
      driver.put(migrationRecord, false, true);
      ResultCode result = movePath(nameSpace, path, replicaRule);
      semaphore.release();
      driver.remove(new Query<>(migrationRecord), MigrationRecord.class);
      AuditLogger.logRuleProcess(currentMethod, nameSpace, path, replicaRule,
          startTime, new Date(), result.getMsg(), defaultMode);
      return new ZoneServiceHttpResponse(result).toString();
    } catch (InterruptedException e) {
      e.printStackTrace();
      AuditLogger.logRuleProcess(currentMethod, nameSpace,
          path, replicaRule, startTime, new Date(),
          ResultCode.INTERRUPTED.getMsg(), defaultMode);
      return new ZoneServiceHttpResponse(ResultCode.INTERRUPTED).toString();
    } catch (IOException e) {
      e.printStackTrace();
      AuditLogger.logRuleProcess(currentMethod, nameSpace,
          path, replicaRule, startTime, new Date(),
          ResultCode.IO_EXCEPTION.getMsg(), defaultMode);
      return new ZoneServiceHttpResponse(ResultCode.IO_EXCEPTION).toString();
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
    String currentMethod =
        Thread.currentThread().getStackTrace()[1].getMethodName();
    if (semaphore.availablePermits() == 0) {
      AuditLogger.logRuleProcess(currentMethod, nameSpace,
          path, replicaRule, startTime, new Date(),
          ResultCode.THREAD_FULL.getMsg(), defaultMode);
      return new ZoneServiceHttpResponse(ResultCode.THREAD_FULL).toString();
    }
    try {
      semaphore.acquire();
      MigrationRecord migrationRecord =
          new MigrationRecord(nameSpace, path, replicaRule);
      driver.put(migrationRecord, false, true);
      ResultCode result = movePath(nameSpace, path, replicaRule);
      semaphore.release();
      driver.remove(new Query<>(migrationRecord), MigrationRecord.class);
      AuditLogger.logRuleProcess(currentMethod, nameSpace, path, replicaRule,
          startTime, new Date(), result.getMsg(), defaultMode);
      return new ZoneServiceHttpResponse(result).toString();
    } catch (InterruptedException e) {
      e.printStackTrace();
      AuditLogger.logRuleProcess(currentMethod, nameSpace,
          path, replicaRule, startTime, new Date(),
          ResultCode.INTERRUPTED.getMsg(), defaultMode);
      return new ZoneServiceHttpResponse(ResultCode.INTERRUPTED).toString();
    } catch (IOException e) {
      e.printStackTrace();
      AuditLogger.logRuleProcess(currentMethod, nameSpace,
          path, replicaRule, startTime, new Date(),
          ResultCode.IO_EXCEPTION.getMsg(), defaultMode);
      return new ZoneServiceHttpResponse(ResultCode.IO_EXCEPTION).toString();
    }
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
    return new ZoneServiceHttpResponse(createUpdateMap(
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
    return new ZoneServiceHttpResponse(createUpdateMap(
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
  public String removePathRuleMap(
      @Context HttpServletRequest hsr,
      @QueryParam("namespace") String nameSpace,
      @PathParam("path") String path) {
    Date startTime = new Date();
    try {
      String threadName = "monitor_" + nameSpace;
      MigrationRecord migrationRecord = new MigrationRecord(nameSpace, path,
          "");
      SignalRecord signalRecord = new SignalRecord(nameSpace, true);
      if (driver.get(new Query<>(migrationRecord),
          MigrationRecord.class) == null) {
        AuditLogger.logRuleProcess(
            "DeletePathRuleMap", nameSpace,
            path, defaultNull, startTime, new Date(),
            ResultCode.NO_MIGRATION_RECORD.getMsg(), "monitor");
        return new ZoneServiceHttpResponse(ResultCode.NO_MIGRATION_RECORD)
            .toString();
      }
      Thread[] ts = new Thread[Thread.activeCount()];
      Thread.enumerate(ts);
      for (Thread tt : ts) {
        if (tt.getName().equals(threadName)) {
          driver.remove(new Query<>(migrationRecord), MigrationRecord.class);
          driver.put(signalRecord, true, false);
          AuditLogger.logRuleProcess(
              "DeletePathRuleMap", nameSpace,
              path, defaultNull, startTime, new Date(),
              ResultCode.SUCCESS.getMsg(), "monitor");
          return new ZoneServiceHttpResponse(ResultCode.SUCCESS).toString();
        }
      }
      AuditLogger.logRuleProcess(
          "DeletePathRuleMap", nameSpace,
          path, defaultNull, startTime, new Date(),
          ResultCode.NO_MIGRATION_RECORD.getMsg(), "monitor");
      return new ZoneServiceHttpResponse(ResultCode.NO_MIGRATION_RECORD)
          .toString();
    } catch (IOException e) {
      e.printStackTrace();
      AuditLogger.logRuleProcess(
          "DeletePathRuleMap", nameSpace,
          path, defaultNull, startTime, new Date(),
          ResultCode.IO_EXCEPTION.getMsg(), "monitor");
      return new ZoneServiceHttpResponse(ResultCode.IO_EXCEPTION).toString();
    }
  }

  /**
   * Move the path with ZoneMover
   * @param nameSpace   URI of the NameNode
   * @param path        the path to apply the rule
   * @param replicaRule the replica rule to apply
   * @return ResultCode
   */
  protected ResultCode movePath(String nameSpace,
      String path, String replicaRule) {
    final Configuration conf = new Configuration();
    final URI namenode = getNamespaceUri(nameSpace, conf);
    final ReplicationRule replicationRule =
        ReplicationRule.parseFromString(replicaRule);
    final List<org.apache.hadoop.fs.Path> paths = new ArrayList<>();
    paths.add(new org.apache.hadoop.fs.Path(path));

    try {
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
  protected Map<ReplicationRule, Set<String>> checkPath(
      String nameSpace, String path, String ratio) {
    Configuration conf = new Configuration();
    URI namenode = getNamespaceUri(nameSpace, conf);
    return ZoneChecker.getReplicaRule(conf, namenode, path,
        Float.parseFloat(ratio));
  }

  /**
   * Create or update path rule map in zk and let ZoneMover process know
   * @param nameSpace    name of the namespace
   * @param path         the path will be updated
   * @param replicaRule  the rule will be applied
   * @param allowCreate  allow create new map by this method or not
   * @return the status of the result
   */
  protected ResultCode createUpdateMap(String nameSpace, String path,
      String replicaRule, boolean allowCreate) {
    Date startTime = new Date();
    try {
      String threadName = "monitor_" + nameSpace;
      SignalRecord signalRecord = new SignalRecord(nameSpace, true);
      MigrationRecord migrationRecord = new MigrationRecord(nameSpace, path,
          replicaRule, "monitor");
      Thread[] ts = new Thread[Thread.activeCount()];
      Thread.enumerate(ts);
      for (Thread tt : ts) {
        //If the thread is existed the new path-rule will add into the thread
        if (tt.getName().equals(threadName)) {
          if (driver.get(new Query<>(migrationRecord),
              MigrationRecord.class) == null & allowCreate) {
            driver.put(migrationRecord, true, false);
            driver.put(signalRecord, true, false);
            AuditLogger.logRuleProcess(
                "setPathRuleMap", nameSpace,
                path, replicaRule, startTime, new Date(),
                ResultCode.CREATE_SUCCESS.getMsg(), "monitor");
            return ResultCode.CREATE_SUCCESS;
          }
          driver.put(migrationRecord, true, false);
          driver.put(signalRecord, true, false);
          AuditLogger.logRuleProcess(
              "updatePathRuleMap", nameSpace,
              path, replicaRule, startTime, new Date(),
              ResultCode.UPDATE_SUCCESS.getMsg(), "monitor");
          return ResultCode.UPDATE_SUCCESS;
        }
      }
      if (!allowCreate) {
        //no thread on this namespace, not allow to create thread by this method
        AuditLogger.logRuleProcess(
            "refreshPathRuleMap", nameSpace,
            path, replicaRule, startTime, new Date(),
            ResultCode.METHOD_ERROR.getMsg(), "monitor");
        return ResultCode.METHOD_ERROR;
      }
      //If there is no monitor thread for this namespace, it will create a new one
      Thread monitorThread = new MonitorThread(threadName, new Configuration(),
          getNamespaceUri(nameSpace, new Configuration()));
      driver.put(migrationRecord, true, true);
      driver.put(signalRecord, true, false);
      monitorThread.start();
      AuditLogger.logRuleProcess(
          "CreatePathRuleMap", nameSpace,
          path, replicaRule, startTime, new Date(),
          ResultCode.CREATE_SUCCESS.getMsg(), "monitor");
      return ResultCode.CREATE_SUCCESS;
    } catch (IOException e) {
      e.printStackTrace();
      AuditLogger.logRuleProcess(
          "CreatePathRuleMap", nameSpace,
          path, replicaRule, startTime, new Date(),
          ResultCode.IO_EXCEPTION.getMsg(), "monitor");
      return ResultCode.IO_EXCEPTION;
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
}