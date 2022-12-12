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

import org.apache.hadoop.yarn.server.globalpolicygenerator.webapp.dao.ClusterWeight;
import org.apache.hadoop.yarn.server.globalpolicygenerator.webapp.dao.ClusterWeights;
import org.apache.hadoop.yarn.server.globalpolicygenerator.webapp.dao.PolicyDeleteRequestInfo;
import org.apache.hadoop.yarn.server.globalpolicygenerator.webapp.dao.PolicyRequestInfo;
import org.apache.hadoop.yarn.server.globalpolicygenerator.webapp.dao.PolicyRequestsInfo;
import org.apache.hadoop.yarn.server.globalpolicygenerator.webapp.dao.PolicyUpdateRequestInfo;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import java.util.ArrayList;

/**
 * Test class to validate the WebService.
 */
public class TestGPGWebServices extends BaseGPGWebServicesTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestGPGWebServices.class);

  private String user = "test1";
  private String queue = "queue1";
  private String clusterName = "cluster1";
  private String weight = "1.0";

  private int HttpOkCode = 200;
  private int HttpErrorCode = 500;

  /**
   * Test that all requests in GPGWebService
   */
  @Test
  public void testGPGWebServicesE2E() throws Exception {

    //test gpgWebServices not null
    GPGWebServices gpgWebServices = getGPGWebServices();
    Assert.assertNotNull(gpgWebServices);

    //test updatePolicy
    PolicyUpdateRequestInfo resContext = new PolicyUpdateRequestInfo();
    PolicyRequestsInfo policyRequestsInfo = new PolicyRequestsInfo();
    ArrayList<PolicyRequestInfo> policyRequestList = new ArrayList<>();
    PolicyRequestInfo policyRequestInfo = new PolicyRequestInfo();
    policyRequestInfo.setQueueName(queue);
    ClusterWeights clusterWeights = new ClusterWeights();
    ArrayList<ClusterWeight> clusterWeight = new ArrayList<>();
    clusterWeight.add(new ClusterWeight(clusterName,weight));
    clusterWeights.setClusterWeight(clusterWeight);
    policyRequestInfo.setClusterWeights(clusterWeights);
    policyRequestList.add(policyRequestInfo);
    policyRequestsInfo.setPolicyRequests(policyRequestList);
    resContext.setPolicyRequests(policyRequestsInfo);
    Response response = updatePolicy(resContext,user);
    Assert.assertEquals(HttpOkCode, response.getStatus());

    //test listPolicy
    Response response2 = listPolicy(queue,user);
    Assert.assertEquals(HttpOkCode, response2.getStatus());

    //test deletePolicy fail
    String queue1 = "queue2";
    PolicyDeleteRequestInfo deletePolicy_1 = new PolicyDeleteRequestInfo();
    deletePolicy_1.setQueueName(queue1);
    Response deleteResponse_1 = deletePolicy(deletePolicy_1, user);
    Assert.assertEquals(HttpErrorCode, deleteResponse_1.getStatus());

    //test deletePolicy success
    PolicyDeleteRequestInfo deletePolicy_2 = new PolicyDeleteRequestInfo();
    deletePolicy_2.setQueueName(queue);
    Response deleteResponse_2 = deletePolicy(deletePolicy_2, user);
    Assert.assertEquals(HttpOkCode, deleteResponse_2.getStatus());
  }

}
