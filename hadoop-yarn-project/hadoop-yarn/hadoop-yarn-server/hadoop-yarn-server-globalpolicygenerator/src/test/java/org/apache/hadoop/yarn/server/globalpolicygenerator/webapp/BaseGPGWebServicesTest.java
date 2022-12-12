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

package org.apache.hadoop.yarn.server.globalpolicygenerator.webapp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.globalpolicygenerator.GlobalPolicyGenerator;
import org.apache.hadoop.yarn.server.globalpolicygenerator.webapp.dao.PolicyDeleteRequestInfo;
import org.apache.hadoop.yarn.server.globalpolicygenerator.webapp.dao.PolicyUpdateRequestInfo;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.mockito.Mockito;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;


/**
 * Base class for all the GPGWebServices test cases. It provides utility
 * methods that can be used by the concrete test case classes.
 *
 */
public abstract class BaseGPGWebServicesTest {

  private YarnConfiguration conf;

  private GlobalPolicyGenerator gpg;

  private GPGWebServices gpgWebService;

  @Before
  public void setUp() {
    this.conf = createConfiguration();

    gpg = spy(new GlobalPolicyGenerator());
    Mockito.doNothing().when(gpg).startWepApp();
    gpgWebService = new GPGWebServices(gpg, conf);
    gpgWebService.setResponse(mock(HttpServletResponse.class));

    gpg.init(conf);
    gpg.start();
  }

  protected YarnConfiguration createConfiguration() {
    YarnConfiguration config = new YarnConfiguration();
    return config;
  }

  @After
  public void tearDown() {
    if (gpg != null) {
      gpg.stop();
    }
  }

  public void setUpConfig() {
    this.conf = createConfiguration();
  }

  protected Configuration getConf() {
    return this.conf;
  }

  protected GPGWebServices getGPGWebServices() {
    Assert.assertNotNull(this.gpgWebService);
    return this.gpgWebService;
  }

  protected Response listPolicy(String queueName, String user)
      throws Exception {
    // HSR is not used here
    return gpgWebService.listPolicy(queueName, createHttpServletRequest(user));
  }

  protected Response updatePolicy(PolicyUpdateRequestInfo resContext,String user)
      throws Exception {
    // HSR is not used here
    return gpgWebService.updatePolicy(resContext, createHttpServletRequest(user));
  }

  protected Response deletePolicy(PolicyDeleteRequestInfo resContext,String user)
      throws Exception {
    // HSR is not used here
    return gpgWebService.deletePolicy(resContext, createHttpServletRequest(user));
  }

  private HttpServletRequest createHttpServletRequest(String user) {
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getRemoteUser()).thenReturn(user);
    return request;
  }

}
