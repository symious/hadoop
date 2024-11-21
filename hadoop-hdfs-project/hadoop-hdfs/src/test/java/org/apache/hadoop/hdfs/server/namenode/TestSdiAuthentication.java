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
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.metrics.RpcMetrics;
import org.apache.hadoop.security.AuthenticationException;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.Objects;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_CLIENT_RPC_SDI_AUTHENTICATION_ENABLED_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SDI_AUTHENTICATION_SILENT_MODE_ENABLED;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_RPC_BLACKLIST_ENABLED_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_RPC_BLACKLIST_FILE;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_RPC_PASSWORD_SHADOW_FILE;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY;
import static org.junit.Assert.assertEquals;

public class TestSdiAuthentication {

  @Test
  public void testSdiAuthEnabled()
      throws IOException, URISyntaxException, InterruptedException {
    File shadowFile = new File(TestNameNodeRpcServer.class
        .getResource("/shadow").toURI());
    String password = "I AM THE DANGER";
    // bypassUser succeed to start and check MiniDFSCluster
    System.setProperty("HADOOP_USER_NAME", "hdfs");
    Configuration conf = new HdfsConfiguration();

    conf.set(HADOOP_CLIENT_RPC_SDI_AUTHENTICATION_ENABLED_KEY, "true");
    conf.set(HADOOP_SECURITY_RPC_PASSWORD_SHADOW_FILE,
        shadowFile.getAbsolutePath());
    UserGroupInformation.setConfiguration(conf);

    MiniDFSCluster cluster = null;
    RpcMetrics metrics;

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
        MiniDFSCluster finalCluster = cluster;
        ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
          DistributedFileSystem fs = finalCluster.getFileSystem();
          final Path root = new Path("/");
          fs.listStatus(root);
          fs.close();
          return null;
        });
        Assert.fail("IOException expected.");
      } catch (IOException ioe) {
        // ok, expected.
        // ok, expected.
        Assert.assertTrue(((RemoteException) ioe).unwrapRemoteException()
            instanceof AuthenticationException);
      }

      try {
        // user with correct password trigger cache
        for (int i = 0; i < 2; i++) {
          String username = "walterWithPassword";
          UserGroupInformation ugi =
              UserGroupInformation.createRemoteUser(username, password);
          MiniDFSCluster finalCluster = cluster;
          ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
            DistributedFileSystem fs = finalCluster.getFileSystem();
            final Path root = new Path("/");
            fs.listStatus(root);
            fs.close();
            return null;
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
  public void testSdiAuthSilentMode()
      throws IOException, URISyntaxException, InterruptedException {
    File shadowFile = new File(TestNameNodeRpcServer.class
        .getResource("/shadow").toURI());
    String password = "I AM THE DANGER";
    // bypassUser succeed to start and check MiniDFSCluster
    System.setProperty("HADOOP_USER_NAME", "hdfs");
    Configuration conf = new HdfsConfiguration();

    conf.set(HADOOP_CLIENT_RPC_SDI_AUTHENTICATION_ENABLED_KEY, "true");
    conf.set(HADOOP_SDI_AUTHENTICATION_SILENT_MODE_ENABLED, "true");
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
        MiniDFSCluster finalCluster = cluster;
        ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
          DistributedFileSystem fs = finalCluster.getFileSystem();
          final Path root = new Path("/");
          fs.listStatus(root);
          fs.close();
          return null;
        });
      } catch (IOException ioe) {
        // Silent mode shouldn't reach here.
        Assert.fail("IOException expected.");
      }
      Assert.assertEquals(1, metrics.getRpcAuthenticationFailures());

    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testBlacklist() throws Exception {
    // BypassUser succeed to start and check MiniDFSCluster.
    System.setProperty("HADOOP_USER_NAME", "hdfs");
    Configuration conf = new HdfsConfiguration();
    String TEST_FILE = "testBlacklist";
    String blackList = Objects.requireNonNull(
        TestSdiAuthentication.class.getClassLoader().getResource(TEST_FILE)).getPath();
    conf.set(HADOOP_SECURITY_RPC_BLACKLIST_FILE, blackList);
    conf.set(HADOOP_CLIENT_RPC_SDI_AUTHENTICATION_ENABLED_KEY, "false");
    // Set RPC service enable blacklist mechanism.
    conf.setBoolean(HADOOP_SECURITY_RPC_BLACKLIST_ENABLED_KEY, true);
    UserGroupInformation.setConfiguration(conf);

    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).build();
      cluster.waitActive();

      String password = "I AM THE DANGER";
      try {
        // UserA is in blacklist and connect router will fail.
        String username = "UserA";
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(username);
        MiniDFSCluster finalCluster = cluster;
        ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
          DistributedFileSystem fs = finalCluster.getFileSystem();
          final Path root = new Path("/");
          fs.listStatus(root);
          fs.close();
          return null;
        });
        Assert.fail("IOException expected.");
      } catch (IOException ioe) {
        Assert.assertTrue(((RemoteException) ioe).unwrapRemoteException()
            instanceof AuthenticationException);
      }

      try {
        // UserB is not in blacklist and connect success.
        for (int i = 0; i < 2; i++) {
          String username = "UserB";
          MiniDFSCluster finalCluster = cluster;
          UserGroupInformation ugi = UserGroupInformation.createRemoteUser(username, password);
          ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
            DistributedFileSystem fs = finalCluster.getFileSystem();
            final Path root = new Path("/");
            fs.listStatus(root);
            fs.close();
            return null;
          });
        }
      } catch (IOException ioe) {
        Assert.fail("Shouldn't reach here.");
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}