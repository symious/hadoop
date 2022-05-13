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

package org.apache.hadoop.yarn.server.globalpolicygenerator;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.client.urlconnection.HttpURLConnectionFactory;
import com.sun.jersey.client.urlconnection.URLConnectionClientHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;
import org.apache.hadoop.yarn.webapp.YarnJacksonJaxbJsonProvider;

import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;

import static javax.servlet.http.HttpServletResponse.SC_OK;

/**
 * GPGUtils contains utility functions for the GPG.
 *
 */
public final class GPGUtils {

  // hide constructor
  private GPGUtils() {
  }

  //Performs an invocation of the the remote RMWebService.
  public static <T> T invokeRMWebService(Configuration conf, String webAddr,
      String path, final Class<T> returnType, String deSelectParam) {
    Client client = Client.create();
    T obj = null;

    WebResource webResource =
        client.resource(webAddr).path("ws/v1/cluster").path(path);
    if (deSelectParam != null) {
      webResource = webResource.queryParam(RMWSConsts.DESELECTS, deSelectParam);
    }
    ClientResponse response = null;
    try {
      response = webResource.accept(MediaType.APPLICATION_XML)
          .get(ClientResponse.class);
      if (response.getStatus() == SC_OK) {
        obj = response.getEntity(returnType);
      } else {
        throw new YarnRuntimeException(
            "Bad response from remote web service: " + response.getStatus());
      }
      return obj;
    } finally {
      if (response != null) {
        response.close();
      }
      client.destroy();
    }
  }

  //Performs an invocation of the the remote RMWebService.
  public static <T> T invokeRMWebService(Configuration conf, String webAddr,
      String path, final Class<T> returnType) {
    return invokeRMWebService(conf, webAddr, path, returnType, null);
  }

  public static Client createClient() {
    ClientConfig cfg = new DefaultClientConfig();
    cfg.getClasses().add(YarnJacksonJaxbJsonProvider.class);
    return new Client(new URLConnectionClientHandler(
        new DummyURLConnectionFactory()), cfg);
  }

  public static ClientResponse getResponse(Client client, URI uri)
      throws Exception {
    ClientResponse resp =
        client.resource(uri).accept(MediaType.APPLICATION_JSON)
            .type(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    if (resp == null ||
        resp.getStatusInfo().getStatusCode() !=
            ClientResponse.Status.OK.getStatusCode()) {
      String msg = new String();
      if (resp != null) {
        msg = String.valueOf(resp.getStatusInfo().getStatusCode());
      }
      throw new IOException("Incorrect response from timeline reader. " +
          "Status=" + msg);
    }
    return resp;
  }

  private static class DummyURLConnectionFactory
      implements HttpURLConnectionFactory {

    @Override
    public HttpURLConnection getHttpURLConnection(final URL url)
        throws IOException {
      try {
        return (HttpURLConnection)url.openConnection();
      } catch (UndeclaredThrowableException e) {
        throw new IOException(e.getCause());
      }
    }
  }

}
