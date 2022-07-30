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

import com.sun.jersey.api.core.ResourceConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.hdfs.server.namenode.NameNodeHttpServer;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress;
import org.apache.hadoop.hdfs.server.zoneservice.web.resources.ZoneMoverWebMigrationRecordMethods;
import org.apache.hadoop.hdfs.server.zoneservice.web.resources.ZoneServiceWebRuleManagementMethods;
import org.apache.hadoop.hdfs.web.resources.AclPermissionParam;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.http.RestCsrfPreventionFilter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_WEBHDFS_REST_CSRF_ENABLED_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_WEBHDFS_REST_CSRF_ENABLED_KEY;

public class ZoneServiceHttpServer {
  private HttpServer2 httpServer;
  private final Configuration conf;

  private InetSocketAddress httpAddress;
  private InetSocketAddress httpsAddress;
  private final InetSocketAddress bindAddress;

  public static final String STARTUP_PROGRESS_ATTRIBUTE_KEY = "startup.progress";
  private static final String PATH_PREFIX = "/zs";

  ZoneServiceHttpServer(Configuration conf, InetSocketAddress bindAddress) {
    this.conf = conf;
    this.bindAddress = bindAddress;
  }

  public static void initZoneService(Configuration conf, String hostname,
      HttpServer2 httpServer2, String jerseyResourcePackage)
      throws IOException {
    AclPermissionParam.setAclPermissionPattern(conf.get(
        HdfsClientConfigKeys.DFS_WEBHDFS_ACL_PERMISSION_PATTERN_KEY,
        HdfsClientConfigKeys.DFS_WEBHDFS_ACL_PERMISSION_PATTERN_DEFAULT));

    // add authentication filter for webhdfs
    final String className = conf.get(
        DFSConfigKeys.DFS_WEBHDFS_AUTHENTICATION_FILTER_KEY,
        DFSConfigKeys.DFS_WEBHDFS_AUTHENTICATION_FILTER_DEFAULT);

    final String pathSpec = PATH_PREFIX + "/*";

    Map<String, String> params = NameNodeHttpServer
        .getAuthFilterParams(conf, hostname);
    String httpKeytab = conf.get(DFSUtil.getSpnegoKeytabKey(conf,
        DFSConfigKeys.DFS_ZONESERVICE_KEYTAB_FILE_KEY));
    if (httpKeytab != null && !httpKeytab.isEmpty()) {
      params.put(
          DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_KEYTAB_KEY,
          httpKeytab);
    } else if (UserGroupInformation.isSecurityEnabled()) {
      HttpServer2.LOG.error(
          "WebHDFS and security are enabled, but configuration property '" +
              DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_KEYTAB_KEY +
              "' is not set.");
    }
    HttpServer2.defineFilter(httpServer2.getWebAppContext(), className, className,
        params, new String[] { pathSpec });
    HttpServer2.LOG.info("Added filter '" + className + "' (class=" + className
        + ")");


    // add REST CSRF prevention filter
    if (conf.getBoolean(DFS_WEBHDFS_REST_CSRF_ENABLED_KEY,
        DFS_WEBHDFS_REST_CSRF_ENABLED_DEFAULT)) {
      Map<String, String> restCsrfParams = RestCsrfPreventionFilter
          .getFilterParams(conf, "dfs.webhdfs.rest-csrf.");
      String restCsrfClassName = RestCsrfPreventionFilter.class.getName();
      HttpServer2.defineFilter(httpServer2.getWebAppContext(),
          restCsrfClassName, restCsrfClassName, restCsrfParams,
          new String[] {pathSpec});
    }

    // add webhdfs packages
    final Map<String, String> resourceParams = new HashMap<>();
    resourceParams.put(ResourceConfig.FEATURE_MATCH_MATRIX_PARAMS, "true");
    httpServer2.addJerseyResourcePackage(
        jerseyResourcePackage, pathSpec, resourceParams);
  }

  /**
   * @see DFSUtil#getHttpPolicy(org.apache.hadoop.conf.Configuration)
   * for information related to the different configuration options and
   * Http Policy is decided.
   */
  void start() throws IOException {
    HttpConfig.Policy policy = DFSUtil.getHttpPolicy(conf);

    final String httpsAddrString = conf.getTrimmed(
        DFSConfigKeys.DFS_ZONESERVICE_HTTPS_ADDRESS_KEY,
        DFSConfigKeys.DFS_ZONESERVICE_HTTPS_ADDRESS_DEFAULT);
    InetSocketAddress httpsAddr = NetUtils.createSocketAddr(httpsAddrString);

    if (httpsAddr != null) {
      // If DFS_ZONESERVICE_HTTPS_BIND_HOST_KEY exists then it overrides the
      // host name portion of DFS_ZONESERVICE_HTTPS_ADDRESS_KEY.
      final String bindHost =
          conf.getTrimmed(DFSConfigKeys.DFS_ZONESERVICE_HTTPS_BIND_HOST_KEY);
      if (bindHost != null && !bindHost.isEmpty()) {
        httpsAddr = new InetSocketAddress(bindHost, httpsAddr.getPort());
      }
    }

    HttpServer2.Builder builder = DFSUtil.httpServerTemplateForNNAndJN(conf,
        bindAddress, httpsAddr, "hdfs",
        DFSConfigKeys.DFS_ZONESERVICE_KERBEROS_INTERNAL_SPNEGO_PRINCIPAL_KEY,
        DFSConfigKeys.DFS_ZONESERVICE_KEYTAB_FILE_KEY);

    final boolean xFrameEnabled = conf.getBoolean(
        DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED,
        DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED_DEFAULT);

    final String xFrameOptionValue = conf.getTrimmed(
        DFSConfigKeys.DFS_XFRAME_OPTION_VALUE,
        DFSConfigKeys.DFS_XFRAME_OPTION_VALUE_DEFAULT);

    builder.configureXFrame(xFrameEnabled).setXFrameOption(xFrameOptionValue);

    httpServer = builder.build();

    initZoneService(conf, bindAddress.getHostName(), httpServer,
        ZoneServiceWebRuleManagementMethods.class.getPackage().getName()
            + ";" + ZoneMoverWebMigrationRecordMethods.class.getPackage().getName());

    httpServer.setAttribute(JspHelper.CURRENT_CONF, conf);
    httpServer.start();

    int connIdx = 0;
    if (policy.isHttpEnabled()) {
      httpAddress = httpServer.getConnectorAddress(connIdx++);
      conf.set(DFSConfigKeys.DFS_ZONESERVICE_HTTP_ADDRESS_KEY,
          NetUtils.getHostPortString(httpAddress));
    }

    if (policy.isHttpsEnabled()) {
      httpsAddress = httpServer.getConnectorAddress(connIdx);
      conf.set(DFSConfigKeys.DFS_ZONESERVICE_HTTPS_ADDRESS_KEY,
          NetUtils.getHostPortString(httpsAddress));
    }
  }

  InetSocketAddress getHttpAddress() {
    return httpAddress;
  }

  InetSocketAddress getHttpsAddress() {
    return httpsAddress;
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

  /**
   * Sets startup progress of zoneservice for use by servlets.
   *
   * @param prog StartupProgress to set
   */
  void setStartupProgress(StartupProgress prog) {
    httpServer.setAttribute(STARTUP_PROGRESS_ATTRIBUTE_KEY, prog);
  }
}
