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
package org.apache.hadoop.hdfs.server.federation.resolver.order;

import org.apache.hadoop.hdfs.server.federation.resolver.PathLocation;

/**
 * Order the destinations based on suffix path,
 * That resolves the first namespace for a given path using the SUFFIX order.
 * if the PathLocation has multiple destinations,
 * it falls back to the HashResolver's implementation.
 */
public class SuffixResolver extends HashResolver {

  /**
   * Retrieves the first namespace for the given path and PathLocation using the SUFFIX order.
   * If the PathLocation has multiple destinations,
   * it falls back to the HashResolver's implementation.
   *
   * @param path Path to check.
   * @param loc Federated location with multiple destinations.
   * @return The first namespace resolved for the path.
   */
  @Override
  public String getFirstNamespace(final String path, final PathLocation loc) {
    if (loc.getDestinations().size() > 1) {
      return super.getFirstNamespace(path, loc);
    }
    return loc.getDestinations().get(0).getNameserviceId();
  }
}
