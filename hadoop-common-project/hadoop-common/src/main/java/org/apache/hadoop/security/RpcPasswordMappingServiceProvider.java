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
package org.apache.hadoop.security;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;

import java.io.IOException;

/**
 * An interface for the implementation of a user-to-rpcPassword mapping service
 * used by {@link RpcPassword}.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface RpcPasswordMappingServiceProvider {
    public static final String RPC_PASSWORD_MAPPING_CONFIG_PREFIX = CommonConfigurationKeysPublic.HADOOP_SECURITY_RPC_PASSWORD_MAPPING;

    /**
     * Get rpc password of a given user.
     * Returns null in case of non-existing user
     * @param user User's name
     * @return password of user
     * @throws IOException
     */
    public String getRpcPassword(String user) throws IOException;
    /**
     * Check if it is a bypass user.
     * @param user User's name
     * @return bypass user or not
     * @throws IOException
     */
    public boolean isBypassUser(String user) throws IOException;

    /**
     * Refresh the cache.
     * @param force force refresh
     * @throws IOException
     */
    public void cacheRefresh(boolean force) throws IOException;
}

