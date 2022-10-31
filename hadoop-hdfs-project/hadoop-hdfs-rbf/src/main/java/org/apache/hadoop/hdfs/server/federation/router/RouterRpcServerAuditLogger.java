/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 *     http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.federation.router;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.hdfs.server.namenode.DefaultAuditLogger;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.TokenIdentifier;

import java.net.InetAddress;
import java.util.Arrays;

public class RouterRpcServerAuditLogger extends DefaultAuditLogger {
  public static final Log auditLog = LogFactory.getLog(
      RouterRpcServer.class.getName() + ".audit");

  @Override
  public void initialize(Configuration conf) {
    if (conf.getBoolean(RBFConfigKeys.DFS_ROUTER_AUDIT_LOG_ASYNC_KEY,
        RBFConfigKeys.DFS_ROUTER_AUDIT_LOG_ASYNC_DEFAULT)) {
      RouterRpcServer.LOG.info("Enabling async auditlog");
      enableAsyncAuditLog(auditLog, RouterRpcServer.LOG);
    }
    isCallerContextEnabled = conf.getBoolean(
        CommonConfigurationKeysPublic.HADOOP_CALLER_CONTEXT_ENABLED_KEY,
        CommonConfigurationKeysPublic.HADOOP_CALLER_CONTEXT_ENABLED_DEFAULT);
    callerContextMaxLen = conf.getInt(
        CommonConfigurationKeysPublic.HADOOP_CALLER_CONTEXT_MAX_SIZE_KEY,
        CommonConfigurationKeysPublic.HADOOP_CALLER_CONTEXT_MAX_SIZE_DEFAULT);
    callerSignatureMaxLen = conf.getInt(
        CommonConfigurationKeysPublic.HADOOP_CALLER_CONTEXT_SIGNATURE_MAX_SIZE_KEY,
        CommonConfigurationKeysPublic.HADOOP_CALLER_CONTEXT_SIGNATURE_MAX_SIZE_DEFAULT);
    logTokenTrackingId = conf.getBoolean(
        RBFConfigKeys.DFS_ROUTER_AUDIT_LOG_TOKEN_TRACKING_ID_KEY,
        RBFConfigKeys.DFS_ROUTER_AUDIT_LOG_TOKEN_TRACKING_ID_DEFAULT);

    debugCmdSet.addAll(Arrays.asList(conf.getTrimmedStrings(
        RBFConfigKeys.DFS_ROUTER_AUDIT_LOG_DEBUG_CMDLIST)));
  }

  public void logAuditEvent(
      boolean succeeded, String userName, InetAddress addr, String cmd,
      String src, String dst, FileStatus status, CallerContext callerContext,
      UserGroupInformation ugi, DelegationTokenSecretManager dtSecretManager,
      String invokeType) {

    if (auditLog.isDebugEnabled() || (auditLog.isInfoEnabled() && !debugCmdSet.contains(cmd))) {
      final StringBuilder sb = STRING_BUILDER.get();
      src = StringEscapeUtils.escapeJava(src);
      dst = StringEscapeUtils.escapeJava(dst);
      sb.setLength(0);
      sb.append("allowed=").append(succeeded).append("\t");
      sb.append("ugi=").append(userName).append("\t");
      sb.append("ip=").append(addr).append("\t");
      sb.append("cmd=").append(cmd).append("\t");
      sb.append("src=").append(src).append("\t");
      sb.append("dst=").append(dst).append("\t");
      if (null == invokeType) {
        sb.append("invokeType=null").append("\t");
      } else {
        sb.append("invokeType=").append(invokeType).append("\t");
      }
      if (null == status) {
        sb.append("perm=null");
      } else {
        sb.append("perm=");
        sb.append(status.getOwner()).append(":");
        sb.append(status.getGroup()).append(":");
        sb.append(status.getPermission());
      }
      if (logTokenTrackingId) {
        sb.append("\t").append("trackingId=");
        String trackingId = null;
        if (ugi != null && dtSecretManager != null
            && ugi.getAuthenticationMethod() == UserGroupInformation.AuthenticationMethod.TOKEN) {
          for (TokenIdentifier tid: ugi.getTokenIdentifiers()) {
            if (tid instanceof DelegationTokenIdentifier) {
              DelegationTokenIdentifier dtid = (DelegationTokenIdentifier)tid;
              trackingId = dtSecretManager.getTokenTrackingId(dtid);
              break;
            }
          }
        }
        sb.append(trackingId);
      }
      sb.append("\t").append("proto=");
      sb.append(Server.getProtocol());
      if (isCallerContextEnabled && callerContext != null && callerContext.isContextValid()) {
        sb.append("\t").append("callerContext=");
        if (callerContext.getContext().length() > callerContextMaxLen) {
          sb.append(callerContext.getContext(), 0, callerContextMaxLen);
        } else {
          sb.append(callerContext.getContext());
        }
        if (callerContext.getSignature() != null &&
            callerContext.getSignature().length > 0 &&
            callerContext.getSignature().length <= callerSignatureMaxLen) {
          sb.append(":");
          sb.append(new String(callerContext.getSignature(), CallerContext.SIGNATURE_ENCODING));
        }
      }
      logAuditMessage(sb.toString());
    }
  }

  @Override
  public void logAuditEvent(boolean succeeded, String userName,
      InetAddress addr, String cmd, String src, String dst,
      FileStatus status, CallerContext callerContext,
      UserGroupInformation ugi, DelegationTokenSecretManager dtSecretManager) {
    this.logAuditEvent(succeeded, userName, addr, cmd, src, dst, status,
        callerContext, ugi, dtSecretManager, null);
  }

  @Override
  public void logAuditEvent(boolean succeeded, String userName,
      InetAddress addr, String cmd, String src, String dst,
      FileStatus status, UserGroupInformation ugi,
      DelegationTokenSecretManager dtSecretManager) {
    this.logAuditEvent(succeeded, userName, addr, cmd, src, dst, status,
        null /*CallerContext*/, ugi, dtSecretManager);
  }

  public void logAuditMessage(String message) {
    auditLog.info(message);
  }
}
