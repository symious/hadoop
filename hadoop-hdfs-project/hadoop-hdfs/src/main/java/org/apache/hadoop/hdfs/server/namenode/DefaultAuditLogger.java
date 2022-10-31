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

package org.apache.hadoop.hdfs.server.namenode;

import java.net.InetAddress;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Appender;
import org.apache.log4j.AsyncAppender;
import org.apache.log4j.Logger;

/**
 * This class provides an interface for Namenode and Router to Audit events
 * information. This class can be extended and can be used when no access logger
 * is defined in the config file.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class DefaultAuditLogger extends HdfsAuditLogger {
  protected static final ThreadLocal<StringBuilder> STRING_BUILDER =
      new ThreadLocal<StringBuilder>() {
        @Override
        protected StringBuilder initialValue() {
          return new StringBuilder();
        }
      };

  protected volatile boolean isCallerContextEnabled;

  /** The maximum bytes a caller context string can have. */
  protected int callerContextMaxLen;
  protected int callerSignatureMaxLen;

  /** adds a tracking ID for all audit log events. */
  protected boolean logTokenTrackingId;

  /** List of commands to provide debug messages. */
  protected Set<String> debugCmdSet = new HashSet<>();

  /**
   * Enable or disable CallerContext.
   *
   * @param value true, enable CallerContext, otherwise false to disable it.
   */
  void setCallerContextEnabled(final boolean value) {
    isCallerContextEnabled = value;
  }

  /**
   * Get the value indicating if CallerContext is enabled.
   *
   * @return true, if CallerContext is enabled, otherwise false, if it's
   *         disabled.
   */
  boolean getCallerContextEnabled() {
    return isCallerContextEnabled;
  }

  public abstract void initialize(Configuration conf);

  public abstract void logAuditMessage(String message);

  public abstract void logAuditEvent(boolean succeeded, String userName,
      InetAddress addr, String cmd, String src, String dst, FileStatus status,
      UserGroupInformation ugi, DelegationTokenSecretManager dtSecretManager);

  public abstract void logAuditEvent(boolean succeeded, String userName,
      InetAddress addr, String cmd, String src, String dst, FileStatus status,
      CallerContext callerContext, UserGroupInformation ugi,
      DelegationTokenSecretManager dtSecretManager);

  protected void enableAsyncAuditLog(Log auditLog, org.slf4j.Logger LOG) {
    if (!(auditLog instanceof Log4JLogger)) {
      LOG.warn("Log4j is required to enable async auditlog");
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
