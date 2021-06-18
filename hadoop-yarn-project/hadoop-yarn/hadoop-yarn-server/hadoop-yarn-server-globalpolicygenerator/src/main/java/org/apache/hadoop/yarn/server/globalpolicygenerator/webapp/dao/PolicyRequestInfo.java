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

import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.ReservationRequests;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationRequestInfo;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;

/**
 * Simple class representing PolicyRequestInfo
 */
@XmlRootElement(name = "policy-requests")
@XmlAccessorType(XmlAccessType.FIELD)
public class PolicyRequestInfo {

  @XmlElement(name = "queueName")
  private String queueName;

  @XmlElement(name = "clusterWeights")
  private ClusterWeights clusterWeights;

  public PolicyRequestInfo() {
  } // JAXB needs this

  public PolicyRequestInfo(String queueName, ClusterWeights clusterWeights) {
   this.queueName = queueName;
   this.clusterWeights = clusterWeights;
  }

  public String getQueueName() {
    return queueName;
  }

  public void setQueueName(String queueName) {
    this.queueName = queueName;
  }

  public ClusterWeights getClusterWeights() {
    return clusterWeights;
  }

  public void setClusterWeights(ClusterWeights clusterWeights) {
    this.clusterWeights = clusterWeights;
  }

  @Override
  public String toString() {
    return "PolicyRequestInfo{" +
        "queueName='" + queueName + '\'' +
        ", clusterWeights=" + clusterWeights +
        '}';
  }
}