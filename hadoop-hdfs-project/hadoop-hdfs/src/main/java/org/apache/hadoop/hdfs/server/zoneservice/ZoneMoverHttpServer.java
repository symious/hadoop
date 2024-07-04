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

package org.apache.hadoop.hdfs.server.zoneservice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.net.NetUtils;

import java.io.IOException;
import java.net.InetSocketAddress;

public class ZoneMoverHttpServer {

  private HttpServer2 httpServer;
  private final Configuration conf;

  private InetSocketAddress httpAddress;
  private InetSocketAddress httpsAddress;
  private final InetSocketAddress bindAddress;

  ZoneMoverHttpServer(Configuration conf, InetSocketAddress bindAddress) {
    this.conf = conf;
    this.bindAddress = bindAddress;
  }

  /**
   * @see DFSUtil#getHttpPolicy(Configuration)
   * for information related to the different configuration options and
   * Http Policy is decided.
   */
  void start() throws IOException {
    HttpConfig.Policy policy = DFSUtil.getHttpPolicy(conf);

    final String httpsAddrString = conf.getTrimmed(
        DFSConfigKeys.DFS_ZONEMOVER_HTTPS_ADDRESS_KEY,
        DFSConfigKeys.DFS_ZONEMOVER_HTTPS_ADDRESS_DEFAULT);
    InetSocketAddress httpsAddr = NetUtils.createSocketAddr(httpsAddrString);

    if (httpsAddr != null) {
      final String bindHost =
          conf.getTrimmed(DFSConfigKeys.DFS_ZONEMOVER_HTTPS_BIND_HOST_KEY);
      if (bindHost != null && !bindHost.isEmpty()) {
        httpsAddr = new InetSocketAddress(bindHost, httpsAddr.getPort());
      }
    }

    HttpServer2.Builder builder = DFSUtil.httpServerTemplateForNNAndJN(conf,
        bindAddress, httpsAddr, "hdfs",
        DFSConfigKeys.DFS_ZONEMOVER_KERBEROS_INTERNAL_SPNEGO_PRINCIPAL_KEY,
        DFSConfigKeys.DFS_ZONEMOVER_KEYTAB_FILE_KEY);

    final boolean xFrameEnabled = conf.getBoolean(
        DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED,
        DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED_DEFAULT);

    final String xFrameOptionValue = conf.getTrimmed(
        DFSConfigKeys.DFS_XFRAME_OPTION_VALUE,
        DFSConfigKeys.DFS_XFRAME_OPTION_VALUE_DEFAULT);

    builder.configureXFrame(xFrameEnabled).setXFrameOption(xFrameOptionValue);

    httpServer = builder.build();

    httpServer.setAttribute(JspHelper.CURRENT_CONF, conf);
    httpServer.start();

    int connIdx = 0;
    if (policy.isHttpEnabled()) {
      httpAddress = httpServer.getConnectorAddress(connIdx++);
      conf.set(DFSConfigKeys.DFS_ZONEMOVER_HTTP_ADDRESS_KEY,
          NetUtils.getHostPortString(httpAddress));
    }

    if (policy.isHttpsEnabled()) {
      httpsAddress = httpServer.getConnectorAddress(connIdx);
      conf.set(DFSConfigKeys.DFS_ZONEMOVER_HTTPS_ADDRESS_KEY,
          NetUtils.getHostPortString(httpsAddress));
    }
  }

  /**
   * Joins the httpserver.
   */
  public void join() throws InterruptedException {
    if (httpServer != null) {
      httpServer.join();
    }
  }

  void stop() throws Exception {
    if (httpServer != null) {
      httpServer.stop();
    }
  }
}
