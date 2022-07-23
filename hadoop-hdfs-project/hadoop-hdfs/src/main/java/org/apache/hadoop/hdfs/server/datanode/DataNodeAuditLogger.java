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
package org.apache.hadoop.hdfs.server.datanode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.hdfs.server.namenode.DefaultAuditLogger;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Appender;
import org.apache.log4j.AsyncAppender;
import org.apache.log4j.Logger;

import java.net.InetAddress;
import java.util.Collections;
import java.util.List;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_AUDIT_LOG_ASYNC_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_AUDIT_LOG_ASYNC_KEY;

public class DataNodeAuditLogger extends DefaultAuditLogger {
  private static final Log auditLog = LogFactory.getLog(
      DataNodeAuditLogger.class.getName() + ".audit");

  public DataNodeAuditLogger(Configuration conf) {
    if (conf.getBoolean(DFS_DATANODE_AUDIT_LOG_ASYNC_KEY,
        DFS_DATANODE_AUDIT_LOG_ASYNC_DEFAULT)) {
      DataNode.LOG.info("Enabling async auditlog");
      enableAsyncAuditLog();
    }
    initialize(conf);
  }

  @Override
  public void initialize(Configuration conf) {
  }

  @Override
  public void logAuditEvent(boolean succeeded, String userName,
      InetAddress addr, String cmd, String src, String dst,
      FileStatus status, CallerContext callerContext, UserGroupInformation ugi,
      DelegationTokenSecretManager dtSecretManager) {
  }

  @Override
  public void logAuditEvent(boolean succeeded, String userName,
      InetAddress addr, String cmd, String src, String dst,
      FileStatus status, UserGroupInformation ugi,
      DelegationTokenSecretManager dtSecretManager) {
    this.logAuditEvent(succeeded, userName, addr, cmd, src, dst, status,
        null /*CallerContext*/, ugi, dtSecretManager);
  }

  /**
   * Try to out put some information to audit_log.
   * @param remoteDC the DC information of remote client.
   * @param trafficSize the size of the traffic.
   * @param trafficInOrOut traffic in or traffic out.
   * @param isInterDCTraffic True if there is cross-dc traffic, else false.
   *
   */
  public void logAuditEvent(String addr, int port, String remoteDC,
      String cmd, String blockInfo, long trafficSize, String trafficInOrOut,
      String localDC, boolean isInterDCTraffic) {
    if (auditLog.isInfoEnabled()) {
      final StringBuilder sb = STRING_BUILDER.get();
      sb.setLength(0);
      sb.append("ip=").append(addr).append("\t")
          .append("port=").append(port).append("\t")
          .append("remoteDC=").append(remoteDC).append("\t")
          .append("cmd=").append(cmd).append("\t")
          .append("block=").append(blockInfo).append("\t")
          .append("size=").append(trafficSize).append("\t")
          .append("traffic=").append(trafficInOrOut).append("\t")
          .append("localDC=").append(localDC).append("\t")
          .append("isInterDCTraffic=").append(isInterDCTraffic);
      logAuditMessage(sb.toString());
    }
  }

  public void logAuditEvent(String addr, int port, String cmd, String blockInfo) {
    if (auditLog.isInfoEnabled()) {
      final StringBuilder sb = STRING_BUILDER.get();
      sb.setLength(0);
      sb.append("ip=").append(addr).append("\t")
          .append("port=").append(port).append("\t")
          .append("cmd=").append(cmd).append("\t")
          .append("block=").append(blockInfo);
      logAuditMessage(sb.toString());
    }
  }

  public void logAuditMessage(String message) {
    DataNode.LOG.debug("Audit message is {}.", message);
    auditLog.info(message);
  }

  private void enableAsyncAuditLog() {
    if (!(auditLog instanceof Log4JLogger)) {
      DataNode.LOG.warn("Log4j is required to enable async auditlog");
      return;
    }
    Logger logger = ((Log4JLogger)auditLog).getLogger();
    @SuppressWarnings("unchecked")
    List<Appender> appenders = Collections.list(logger.getAllAppenders());
    // failsafe against trying to async it more than once
    if (!appenders.isEmpty() && !(appenders.get(0) instanceof AsyncAppender)) {
      AsyncAppender asyncAppender = new AsyncAppender();
      // change logger to have an async appender containing all the
      // previously configured appenders
      for (Appender appender : appenders) {
        logger.removeAppender(appender);
        asyncAppender.addAppender(appender);
      }
      logger.addAppender(asyncAppender);
    }
  }
}
