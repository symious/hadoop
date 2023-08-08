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
package org.apache.hadoop.hdfs.server.federation.router;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.NameNodeRpcServer;
import org.apache.hadoop.hdfs.server.namenode.TestNameNodeRpcServer;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.metrics.RpcMetrics;
import org.apache.hadoop.security.AuthenticationException;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_CLIENT_RPC_SDI_AUTHENTICATION_ENABLED_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_RPC_PASSWORD_SHADOW_FILE;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.NAMENODES;
import static org.junit.Assert.assertEquals;

public class TestSdiAuthenticationFromRBF {
  @Test
  public void testSdiAuthEnabled() throws Exception {
    File shadowFile = new File(TestNameNodeRpcServer.class.getResource("/shadow").toURI());
    String password = "I AM THE DANGER";
    // bypassUser succeed to start and check MiniDFSCluster
    System.setProperty("HADOOP_USER_NAME", "hdfs");
    Configuration conf = new HdfsConfiguration();

    conf.set(HADOOP_CLIENT_RPC_SDI_AUTHENTICATION_ENABLED_KEY, "true");
    conf.set(HADOOP_SECURITY_RPC_PASSWORD_SHADOW_FILE, shadowFile.getAbsolutePath());
    UserGroupInformation.setConfiguration(conf);

    MiniRouterDFSCluster cluster = null;
    RpcMetrics metrics;

    try {
      cluster = new MiniRouterDFSCluster(true, 1);
      cluster.addNamenodeOverrides(conf);

      // Start NNs and DNs and wait until ready
      cluster.startCluster(conf);

      // Start routers with only an RPC service
      cluster.startRouters();

      // Register and verify all NNs with all routers
      cluster.registerNamenodes();
      cluster.waitNamenodeRegistration();

      // Setup the mount table
      cluster.installMockLocations();

      // Making one Namenodes active per nameservice
      if (cluster.isHighAvailability()) {
        for (String ns : cluster.getNameservices()) {
          cluster.switchToActive(ns, NAMENODES[0]);
          cluster.switchToStandby(ns, NAMENODES[1]);
        }
      }
      cluster.waitActiveNamespaces();
      metrics = ((NameNodeRpcServer) cluster.getCluster().getNameNodeRpc(0)).
          getClientRpcServer().getRpcMetrics();


      MiniRouterDFSCluster finalCluster = cluster;
      try {
        // walterWithoutPassword failed to operation MiniDFSCluster
        String username = "walterWithoutPassword";
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(username);
        ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
          DistributedFileSystem fs = finalCluster.getCluster().getFileSystem(0);
          final Path root = new Path("/target_dir");
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
          UserGroupInformation ugi = UserGroupInformation.createRemoteUser(username, password);
          ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
            DistributedFileSystem fs = finalCluster.getCluster().getFileSystem(0);
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
}
