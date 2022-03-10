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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.ipc.metrics.RpcMetrics;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_RPC_PASSWORD_SHADOW_FILE;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SERVICE_RPC_SDI_AUTHENTICATION_ENABLED;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY;
import static org.junit.Assert.assertEquals;

public class TestSdiAuthentication {

  @Test
  public void testClientRpcSdiAuthEnabled()
      throws IOException, URISyntaxException, InterruptedException {
    File shadowFile = new File(TestNameNodeRpcServer.class
        .getResource("/shadow").toURI());
    String password = "I AM THE DANGER";
    // bypassUser succeed to start and check MiniDFSCluster
    System.setProperty("HADOOP_USER_NAME", "hdfs");
    Configuration conf = new HdfsConfiguration();

    conf.set(HADOOP_SECURITY_RPC_PASSWORD_SHADOW_FILE,
        shadowFile.getAbsolutePath());
    UserGroupInformation.setConfiguration(conf);

    MiniDFSCluster cluster = null;
    RpcMetrics metrics = null;

    try {
      cluster = new MiniDFSCluster.Builder(conf).build();
      cluster.waitActive();
      metrics = ((NameNodeRpcServer) cluster.getNameNodeRpc()).
          getClientRpcServer().getRpcMetrics();

      try {
        // walterWithoutPassword failed to operation MiniDFSCluster
        String username = "walterWithoutPassword";
        UserGroupInformation ugi =
            UserGroupInformation.createRemoteUser(username);
        final MiniDFSCluster finalCluster = cluster;
        ugi.doAs(new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            DistributedFileSystem fs = finalCluster.getFileSystem();
            final Path root = new Path("/");
            fs.listStatus(root);
            fs.close();
            return null;
          }
        });
        Assert.fail("IOException expected.");
      } catch (IOException ioe) {
        // ok, expected.
      }

      try {
        // user with correct password trigger cache
        for (int i = 0; i < 2; i++) {
          String username = "walterWithPassword";
          UserGroupInformation ugi =
              UserGroupInformation.createRemoteUser(username, password);
          final MiniDFSCluster finalCluster = cluster;
          ugi.doAs(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
              DistributedFileSystem fs = finalCluster.getFileSystem();
              final Path root = new Path("/");
              fs.listStatus(root);
              fs.close();
              return null;
            }
          });
        }
      } catch (IOException ioe) {
        Assert.fail("Shouldn't reach here.");
      }
      assertEquals(2, metrics.passwordMatchedCacheTotalRequest());
      assertEquals(1, metrics.passwordMatchedCacheMissCount());
      assertEquals(1, metrics.passwordMatchedCacheHitCount());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testServiceRpcSdiAuthEnabled()
      throws IOException, URISyntaxException, InterruptedException {
    File shadowFile = new File(TestNameNodeRpcServer.class
        .getResource("/shadow").toURI());
    String password = "I AM THE DANGER";
    // bypassUser succeed to start and check MiniDFSCluster
    System.setProperty("HADOOP_USER_NAME", "hdfs");
    Configuration conf = new HdfsConfiguration();

    conf.set(HADOOP_SERVICE_RPC_SDI_AUTHENTICATION_ENABLED, "true");
    conf.set(HADOOP_SECURITY_RPC_PASSWORD_SHADOW_FILE,
        shadowFile.getAbsolutePath());
    conf.set(DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, "localhost:9030");
    UserGroupInformation.setConfiguration(conf);

    MiniDFSCluster cluster = null;
    RpcMetrics clientRpcMetrics = null;
    RpcMetrics serviceRpcMetrics = null;

    try {
      cluster = new MiniDFSCluster.Builder(conf).build();
      cluster.waitActive();
      clientRpcMetrics = ((NameNodeRpcServer) cluster.getNameNodeRpc()).
          getClientRpcServer().getRpcMetrics();
      serviceRpcMetrics = ((NameNodeRpcServer) cluster.getNameNodeRpc()).
          getServiceRpcServer().getRpcMetrics();

      try {
        // walterWithoutPassword failed to operation MiniDFSCluster
        String username = "walterWithoutPassword";
        UserGroupInformation ugi =
            UserGroupInformation.createRemoteUser(username);
        final MiniDFSCluster finalCluster = cluster;
        ugi.doAs(new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            Configuration conf = finalCluster.getConfiguration(0);
            FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:9030/"), conf);
            fs.listStatus(new Path("hdfs://localhost:9030/"));
            return null;
          }
        });
        Assert.fail("IOException expected.");
      } catch (IOException ioe) {
        ioe.printStackTrace();
        // ok, expected.
      }

      try {
        // user with correct password trigger cache
        for (int i = 0; i < 2; i++) {
          String username = "walterWithPassword";
          UserGroupInformation ugi =
              UserGroupInformation.createRemoteUser(username, password);
          final MiniDFSCluster finalCluster = cluster;
          ugi.doAs(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
              Configuration conf = finalCluster.getConfiguration(0);
              FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:9030"), conf);
              fs.listStatus(new Path("hdfs://localhost:9030/"));
              return null;
            }
          });
        }
      } catch (IOException ioe) {
        Assert.fail("Shouldn't reach here.");
      }
      assertEquals(0, clientRpcMetrics.passwordMatchedCacheTotalRequest());
      assertEquals(2, serviceRpcMetrics.passwordMatchedCacheTotalRequest());
      assertEquals(1, serviceRpcMetrics.passwordMatchedCacheMissCount());
      assertEquals(1, serviceRpcMetrics.passwordMatchedCacheHitCount());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}

