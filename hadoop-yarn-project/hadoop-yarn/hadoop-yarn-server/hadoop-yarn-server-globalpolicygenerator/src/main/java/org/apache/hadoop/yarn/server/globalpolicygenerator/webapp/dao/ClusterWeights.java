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

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;

/**
 * Simple class representing a list of ClusterWeight
 */
@XmlRootElement(name = "policy-requests")
@XmlAccessorType(XmlAccessType.FIELD)
public class ClusterWeights {

  @XmlElement(name = "cluster-weight")
  private ArrayList<ClusterWeight> clusterWeight;

  public ClusterWeights() {
  } // JAXB needs this

  public ClusterWeights(ArrayList<ClusterWeight> clusterWeight) {
    this.clusterWeight = clusterWeight;
  }

  public ArrayList<ClusterWeight> getClusterWeight() {
    return clusterWeight;
  }

  public void setClusterWeight(
      ArrayList<ClusterWeight> clusterWeight) {
    this.clusterWeight = clusterWeight;
  }

  @Override
  public String toString() {
    return "ClusterWeights{" +
        "clusterWeight=" + clusterWeight +
        '}';
  }
}