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
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FixedOrderResolver implements OrderedResolver {

  private static final Logger LOG = LoggerFactory.getLogger(FixedOrderResolver.class);

  @Override
  public String getFirstNamespace(String path, PathLocation loc) {
    if (loc == null || loc.getDestinations().isEmpty()) {
      LOG.error("Cannot get namespaces for {}", loc);
      return null;
    }
    String result = null;
    RemoteLocation remoteLocation = loc.getDestinations().get(0);
    if (remoteLocation != null) {
      result = remoteLocation.getNameserviceId();
      LOG.debug("First ns resolution: path={}, ns={}", path, result);
    }
    return result;
  }
}
