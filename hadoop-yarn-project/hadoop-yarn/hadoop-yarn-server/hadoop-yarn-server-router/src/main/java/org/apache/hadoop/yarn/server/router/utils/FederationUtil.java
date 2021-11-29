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
package org.apache.hadoop.yarn.server.router.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.router.fairness.NoRouterRpcFairnessPolicyController;
import org.apache.hadoop.yarn.server.router.fairness.RouterRpcFairnessPolicyController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;

/**
 * Utilities for managing yarn federation.
 */
public final class FederationUtil {

  public static final Class<? extends RouterRpcFairnessPolicyController>
      ROUTER_FAIRNESS_POLICY_CONTROLLER_CLASS_DEFAULT =
      NoRouterRpcFairnessPolicyController.class;

  private static final Logger LOG =
      LoggerFactory.getLogger(FederationUtil.class);

  private static RouterRpcFairnessPolicyController routerRpcFairnessPolicyController;

  private FederationUtil() {
    // Utility Class
  }

  /**
   * Creates an instance of an RouterRpcFairnessPolicyController
   * from the configuration.
   * Use a singleton pattern with dual monitoring
   *
   * @param conf Configuration that defines the fairness controller class.
   * @return Fairness policy controller.
   */
  public static RouterRpcFairnessPolicyController getFairnessPolicyController(
      Configuration conf) {
    if (routerRpcFairnessPolicyController == null) {
      synchronized (FederationUtil.class) {
        if (routerRpcFairnessPolicyController == null) {
          Class<? extends RouterRpcFairnessPolicyController> clazz =
              conf.getClass(
                  YarnConfiguration.ROUTER_FAIRNESS_POLICY_CONTROLLER_CLASS,
                  ROUTER_FAIRNESS_POLICY_CONTROLLER_CLASS_DEFAULT,
                  RouterRpcFairnessPolicyController.class);
          routerRpcFairnessPolicyController =
              newInstance(conf, null, null, clazz);
        }
      }
    }
    return routerRpcFairnessPolicyController;
  }

  /**
   * Create an instance of an interface with a constructor using a context.
   *
   * @param conf Configuration for the class names.
   * @param context Context object to pass to the instance.
   * @param contextClass Type of the context passed to the constructor.
   * @param clazz Class of the object to return.
   * @return New instance of the specified class that implements the desired
   *         interface and a single parameter constructor containing a
   *         StateStore reference.
   */
  private static <T, R> T newInstance(final Configuration conf,
      final R context, final Class<R> contextClass, final Class<T> clazz) {
    try {
      if (contextClass == null) {
        if (conf == null) {
          // Default constructor if no context
          Constructor<T> constructor = clazz.getConstructor();
          return constructor.newInstance();
        } else {
          // Constructor with configuration but no context
          Constructor<T> constructor = clazz.getConstructor(
              Configuration.class);
          return constructor.newInstance(conf);
        }
      } else {
        // Constructor with context
        Constructor<T> constructor = clazz.getConstructor(
            Configuration.class, contextClass);
        return constructor.newInstance(conf, context);
      }
    } catch (ReflectiveOperationException e) {
      LOG.error("Could not instantiate: {}", clazz.getSimpleName(), e);
      return null;
    }
  }

}