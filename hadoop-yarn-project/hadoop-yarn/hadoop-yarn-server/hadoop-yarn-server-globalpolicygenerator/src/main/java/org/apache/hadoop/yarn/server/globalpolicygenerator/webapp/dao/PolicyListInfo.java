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
package org.apache.hadoop.yarn.server.globalpolicygenerator.webapp.dao;

import org.apache.hadoop.yarn.server.federation.store.records.
    SubClusterPolicyConfiguration;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.yarn.server.globalpolicygenerator.webapp.GPGWSConsts;

/**
 * Simple class that represent a list of query policies
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class PolicyListInfo {
  private static final Logger LOG =
      LoggerFactory.getLogger(PolicyListInfo.class);

  @XmlElement(name = "policies")
  private List<PolicyInfo> policies = new ArrayList<>();

  public PolicyListInfo(){
  } // JAXB needs this

  public PolicyListInfo(List<SubClusterPolicyConfiguration> spcList)
      throws JSONException {

    for(SubClusterPolicyConfiguration spc : spcList){
      PolicyInfo policyInfo = new PolicyInfo();
      policyInfo.setQueueName(spc.getQueue());

      //parse clusterWeights from spc params
      ClusterWeights clusterWeights = new ClusterWeights();
      ArrayList<ClusterWeight> clusterWeightList = new ArrayList<>();
      String allParams = new String(getByteArray(spc.getParams()));
      JSONObject jsonObject = new JSONObject(allParams);
      JSONObject routerJsonObject = jsonObject.
          getJSONObject(GPGWSConsts.WEIGHTS_FIELD);
      try{
        JSONObject weightsJsonObject = routerJsonObject.getJSONObject(
            GPGWSConsts.ENTRY_FIELD);
        String clusterName = weightsJsonObject.getJSONObject(
            GPGWSConsts.KEY_FIELD).getString(GPGWSConsts.ID_FIELD);
        double weight = weightsJsonObject.getDouble(GPGWSConsts.VALUE_FIELD);
        clusterWeightList.add(new ClusterWeight(clusterName, (float) weight));
      }catch (Exception e){
        LOG.error("Can't parse JsonObject entry, try to use JSONArray.",e);
        JSONArray weightsJsonArray = routerJsonObject.
            getJSONArray(GPGWSConsts.ENTRY_FIELD);
        for(int i=0;i<weightsJsonArray.length();i++){
          JSONObject weightPair = weightsJsonArray.getJSONObject(i);
          String clusterName = weightPair.getJSONObject(GPGWSConsts.KEY_FIELD).
              getString(GPGWSConsts.ID_FIELD);
          double weight = weightPair.getDouble(GPGWSConsts.VALUE_FIELD);
          clusterWeightList.add(new ClusterWeight(clusterName, (float) weight));
        }
      }
      clusterWeights.setClusterWeight(clusterWeightList);
      policyInfo.setClusterWeights(clusterWeights);

      policies.add(policyInfo);
    }

  }

  public List<PolicyInfo> getPolicies() {
    return policies;
  }

  public void setPolicies(
      List<PolicyInfo> policies) {
    this.policies = policies;
  }

  private static byte[] getByteArray(ByteBuffer bb) {
    byte[] ba = new byte[bb.limit()];
    bb.get(ba);
    return ba;
  }

  @Override
  public String toString() {
    return "PolicyListInfo{" +
        "policies=" + policies +
        '}';
  }
}
