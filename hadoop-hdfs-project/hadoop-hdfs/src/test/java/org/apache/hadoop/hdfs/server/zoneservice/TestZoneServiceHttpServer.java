package org.apache.hadoop.hdfs.server.zoneservice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.TestNameNodeHttpServer;
import org.apache.hadoop.http.HttpConfig;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.net.InetSocketAddress;

@RunWith(value = Parameterized.class)
public class TestZoneServiceHttpServer extends TestNameNodeHttpServer {
  private final HttpConfig.Policy policy;

  public TestZoneServiceHttpServer(HttpConfig.Policy policy) {
    super(policy);
    this.policy = policy;
  }

  @Test
  public void testHttpPolicy() throws Exception {
    conf.set(DFSConfigKeys.DFS_HTTP_POLICY_KEY, policy.name());
    conf.set(DFSConfigKeys.DFS_ZONESERVICE_HTTPS_ADDRESS_KEY, "localhost:0");

    InetSocketAddress addr = InetSocketAddress.createUnresolved("localhost", 0);
    ZoneServiceHttpServer server = null;
    try {
      server = new ZoneServiceHttpServer(conf, addr);
      server.start();

      Assert.assertTrue(implies(policy.isHttpEnabled(),
          canAccess("http", server.getHttpAddress())));
      Assert.assertTrue(implies(!policy.isHttpEnabled(),
          server.getHttpAddress() == null));

      Assert.assertTrue(implies(policy.isHttpsEnabled(),
          canAccess("https", server.getHttpsAddress())));
      Assert.assertTrue(implies(!policy.isHttpsEnabled(),
          server.getHttpsAddress() == null));

    } finally {
      if (server != null) {
        server.stop();
      }
    }
  }
}