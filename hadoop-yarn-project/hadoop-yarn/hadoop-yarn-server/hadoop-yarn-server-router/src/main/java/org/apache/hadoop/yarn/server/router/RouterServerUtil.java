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

package org.apache.hadoop.yarn.server.router;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Common utility methods used by the Router server.
 *
 */
@Private
@Unstable
public final class RouterServerUtil {

  /** Disable constructor. */
  private RouterServerUtil() {
  }

  public static final Logger LOG =
      LoggerFactory.getLogger(RouterServerUtil.class);

  /**
   * Throws an exception due to an error.
   *
   * @param errMsg the error message
   * @param t the throwable raised in the called class.
   * @throws YarnException on failure
   */
  @Public
  @Unstable
  public static void logAndThrowException(String errMsg, Throwable t)
      throws YarnException {
    if (t != null) {
      LOG.error(errMsg, t);
      throw new YarnException(errMsg, t);
    } else {
      LOG.error(errMsg);
      throw new YarnException(errMsg);
    }
  }

  /**
   * Set User information.
   *
   * If the username is empty, we will use the Yarn Router user directly.
   * Do not create a proxy user if userName matches the userName on current UGI.
   *
   * @param userName userName.
   * @return UserGroupInformation.
   */
  public static UserGroupInformation setupUser(final String userName) {
    UserGroupInformation user = null;
    try {
      // If userName is empty, we will return UserGroupInformation.getCurrentUser.
      // Do not create a proxy user if user name matches the user name on
      // current UGI
      if (userName == null || userName.trim().isEmpty()) {
        user = UserGroupInformation.getCurrentUser();
      } else if (UserGroupInformation.isSecurityEnabled()) {
        user = UserGroupInformation.createProxyUser(userName, UserGroupInformation.getLoginUser());
      } else if (userName.equalsIgnoreCase(UserGroupInformation.getCurrentUser().getUserName())) {
        user = UserGroupInformation.getCurrentUser();
      } else {
        user = UserGroupInformation.createProxyUser(userName,
            UserGroupInformation.getCurrentUser());
      }
      return user;
    } catch (IOException e) {
      throw RouterServerUtil.logAndReturnYarnRunTimeException(e,
          "Error while creating Router Service for user : %s.", user);
    }
  }

  /**
   * Throws an YarnRuntimeException due to an error.
   *
   * @param t the throwable raised in the called class.
   * @param errMsgFormat the error message format string.
   * @param args referenced by the format specifiers in the format string.
   * @return YarnRuntimeException
   */
  @Public
  @Unstable
  public static YarnRuntimeException logAndReturnYarnRunTimeException(
      Throwable t, String errMsgFormat, Object... args) {
    String msg = String.format(errMsgFormat, args);
    if (t != null) {
      String newErrMsg = getErrorMsg(msg, t);
      LOG.error(newErrMsg, t);
      return new YarnRuntimeException(newErrMsg, t);
    } else {
      LOG.error(msg);
      return new YarnRuntimeException(msg);
    }
  }

  private static String getErrorMsg(String errMsg, Throwable t) {
    if (t.getMessage() != null) {
      return errMsg + "" + t.getMessage();
    }
    return errMsg;
  }
}