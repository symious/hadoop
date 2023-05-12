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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.hadoop.hdfs.server.federation.metrics.RBFMetrics;
import org.apache.hadoop.hdfs.server.federation.resolver.FileSubclusterResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.PathLocation;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.federation.store.records.MembershipState;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.hadoop.thirdparty.com.google.common.net.HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN;
import static io.netty.handler.codec.http.HttpHeaders.Names.ACCESS_CONTROL_ALLOW_METHODS;

public class NSServlet extends HttpServlet {

  private final Logger LOG = LoggerFactory.getLogger(NSServlet.class);
  /** Default serial identifier. */
  private static final long serialVersionUID = 1L;

  static final String SERVLET_NAME = "nsInfo";
  static final String PATH_SPEC = "/nsInfo";

  private JsonFactory jsonFactory;

  /**
   * Initialize this servlet.
   */
  @Override
  public void init() {
    jsonFactory = new JsonFactory();
  }

  /**
   * Check whether this instance is the Active one.
   * @param req HTTP request
   * @param resp HTTP response to write to
   */
  @Override
  public void doGet(final HttpServletRequest req, final HttpServletResponse resp) throws IOException {
    JsonGenerator jg = null;
    PrintWriter writer = null;
    try {
      writer = resp.getWriter();

      resp.setContentType("application/json; charset=utf8");
      resp.setHeader(ACCESS_CONTROL_ALLOW_METHODS, "GET");
      resp.setHeader(ACCESS_CONTROL_ALLOW_ORIGIN, "*");

      jg = jsonFactory.createGenerator(writer);
      jg.disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
      jg.useDefaultPrettyPrinter();

      final ServletContext context = getServletContext();
      final Router router = RouterHttpServer.getRouterFromContext(context);
      String path = req.getParameter("path");
      String showAllMountPoints = req.getParameter("showAllMountPoints");
      if (path != null) {
        String isFile = req.getParameter("isFile");
        jg.writeStartObject();
        List<String> nsList = getNsInfoForPath(path, router, isFile);
        LOG.debug("NSServlet path is {}, nsList is {}.", path, nsList);
        jg.writeFieldName("NSInfo");
        jg.writeStartArray();
        for (String ns : nsList) {
          jg.writeString(ns);
        }
        jg.writeEndArray();
        jg.writeEndObject();
        resp.setStatus(HttpServletResponse.SC_OK);
      } else if (showAllMountPoints != null) {
        List<MountTable> allMountTables = getAllMountTable(router);
        LOG.debug("NSServlet showAllMountPoints, allMountTables is {}.", allMountTables);
        jg.writeStartObject();
        jg.writeFieldName("AllMountTables");
        jg.writeStartArray();
        for (MountTable table : allMountTables) {
          jg.writeStartObject();
          jg.writeObjectField("SourcePath", table.getSourcePath());
          jg.writeObjectField("DestOrder", table.getDestOrder().name());
          List<RemoteLocation> destLocations = table.getDestinations();
          jg.writeFieldName("Destinations");
          jg.writeStartArray();
          for (RemoteLocation location : destLocations) {
            jg.writeStartObject();
            jg.writeStringField("Namespace", location.getNameserviceId());
            jg.writeStringField("DestinationPath", location.getDest());
            jg.writeEndObject();
          }
          jg.writeEndArray();
          jg.writeEndObject();
        }
        jg.writeEndArray();
        jg.writeEndObject();
        resp.setStatus(HttpServletResponse.SC_OK);
      } else {
        List<MembershipState> membershipStates = getAllMembershipState(router);
        LOG.debug("NSServlet path is null, membershipStates is {}.", membershipStates);
        jg.writeStartObject();
        jg.writeFieldName("AllNamespaces");
        jg.writeStartArray();
        if (membershipStates != null && membershipStates.size() >0) {
          membershipStates.sort(MembershipState.NAME_COMPARATOR);
          for (MembershipState membershipState : membershipStates) {
            jg.writeStartObject();
            jg.writeObjectField("NameServiceId", membershipState.getNameserviceId());
            jg.writeObjectField("BlockPoolId", membershipState.getBlockPoolId());
            jg.writeObjectField("ClusterId", membershipState.getClusterId());
            jg.writeEndObject();
          }
        }
        jg.writeEndArray();
        jg.writeEndObject();
        resp.setStatus(HttpServletResponse.SC_OK);
      }
      resp.getWriter().flush();
    } finally {
      if (jg != null) {
        jg.close();
      }
      if (writer != null) {
        writer.close();
      }
    }
  }

  private List<MountTable> getAllMountTable(Router router) throws IOException {
    FileSubclusterResolver resolver = router.getSubclusterResolver();
    if (resolver instanceof MountTableResolver) {
      return ((MountTableResolver) resolver).getMounts("/");
    }
    return new ArrayList<>();
  }

  private List<MembershipState> getAllMembershipState(Router router) throws IOException {
    List<MembershipState> result = new ArrayList<>();
    Set<String> filter = new HashSet<>();
    RBFMetrics rbfMetrics = router.getMetrics();
    if (rbfMetrics != null) {
      for (MembershipState membershipState : rbfMetrics.getAllMembershipState()) {
        if (!filter.contains(membershipState.getBlockPoolId())) {
          result.add(membershipState);
          filter.add(membershipState.getBlockPoolId());
        }
      }
      return result;
    } else {
      LOG.warn("RbfMetric is null....");
    }
    return null;
  }

  private List<String> getNsInfoForPath(String path, Router router, String isFile)
      throws IOException {
    List<String> result = new ArrayList<>();
    RouterRpcServer rpcServer = router.getRpcServer();
    if (rpcServer != null) {
      FileSubclusterResolver resolver = rpcServer.getSubclusterResolver();
      if (resolver != null) {
        if ("true".equals(isFile)) {
          String ns = rpcServer.getFile2NS(path);
          if (ns != null) {
            result.add(ns);
          }
        } else {
          PathLocation pathLocation = resolver.getDestinationForPath(path);
          if (pathLocation != null) {
            for (RemoteLocation location : pathLocation.getDestinations()) {
              result.add(location.getNameserviceId());
            }
          }
        }
      }
    } else {
      LOG.warn("RpcServer is null....");
    }
    return result;
  }
}
