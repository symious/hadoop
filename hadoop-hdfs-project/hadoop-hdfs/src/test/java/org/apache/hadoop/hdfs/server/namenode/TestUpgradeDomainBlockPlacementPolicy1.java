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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.AddBlockFlag;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.net.DFSNetworkTopology;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.BlockType;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicies;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyWithUpgradeDomain;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyWithUpgradeDomainForRackFaultTolerant;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.util.Time;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_PLACEMENT_EC_CLASSNAME_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_REPLICATOR_CLASSNAME_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_KEY;

/**
 * End-to-end test case for upgrade domain
 * The test configs upgrade domain for nodes via admin json
 * config file and put some nodes to decommission state.
 * The test then verifies replicas are placed on the nodes that
 * satisfy the upgrade domain policy.
 *
 */
public class TestUpgradeDomainBlockPlacementPolicy1 {
  private final Map<DatanodeInfo, String> dnRacks = new ConcurrentHashMap<>();
  private final Map<DatanodeInfo, String> dnDomains = new ConcurrentHashMap<>();

  private final Map<DatanodeInfo, Integer> dnMaps = new ConcurrentHashMap<>();
  private final Map<String, Integer> domainMaps = new ConcurrentHashMap<>();

  private final BlockPlacementPolicies blockPlacementPolicies;
  private final NetworkTopology networkTopology;

  public TestUpgradeDomainBlockPlacementPolicy1(
      String dfsHosts, boolean enableUpgradeDomain, String testDC)
      throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_KEY, false);
    this.networkTopology = initNetworkTopology(conf, dfsHosts, testDC);
    this.blockPlacementPolicies = initBlockPlacementPolicies(conf, enableUpgradeDomain);
  }

  private BlockPlacementPolicies initBlockPlacementPolicies(
      Configuration conf, boolean enableUpgradeDomain) {
    if (enableUpgradeDomain) {
      conf.set(DFS_BLOCK_REPLICATOR_CLASSNAME_KEY,
          BlockPlacementPolicyWithUpgradeDomain.class.getName());
      conf.set(DFS_BLOCK_PLACEMENT_EC_CLASSNAME_KEY,
          BlockPlacementPolicyWithUpgradeDomainForRackFaultTolerant.class.getName());
    }
    return new BlockPlacementPolicies(conf, null, this.networkTopology, null);
  }

  private void chooseTargetNodeForNewBlocks(boolean forEC) {

    EnumSet<AddBlockFlag> flags = EnumSet.noneOf(AddBlockFlag.class);
    final byte hotId = HdfsConstants.HOT_STORAGE_POLICY_ID;
    BlockStoragePolicy storagePolicy = new BlockStoragePolicy(hotId,
        HdfsConstants.HOT_STORAGE_POLICY_NAME,
        new StorageType[]{StorageType.DISK}, StorageType.EMPTY_ARRAY,
        new StorageType[]{StorageType.ARCHIVE});


    BlockPlacementPolicy contiguousPolicy =
        this.blockPlacementPolicies.getPolicy(BlockType.CONTIGUOUS);
    BlockPlacementPolicy stripePolicy =
        this.blockPlacementPolicies.getPolicy(BlockType.STRIPED);

    BlockPlacementPolicy policy = forEC ? stripePolicy : contiguousPolicy;

    long startTime = Time.monotonicNow();
    int index = 0;
    while (index++ <= 1000000) {
      Node client = this.networkTopology.chooseRandom(NodeBase.ROOT);
      //LOG.info("Client for {} is {}.", index, client);
      DatanodeStorageInfo[] datanodeStorageInfos = policy.chooseTarget(
          "mockpath", 3, client, new HashSet<>(), (1024 * 1024),
          null, storagePolicy, flags);
      for (DatanodeStorageInfo storageInfo : datanodeStorageInfos) {
        DatanodeDescriptor dn = storageInfo.getDatanodeDescriptor();
        int value = dnMaps.getOrDefault(dn, 0);
        value += 1;
        dnMaps.put(dn, value);

        int domainValue = domainMaps.getOrDefault(dn.getUpgradeDomain(), 0);
        domainValue += 1;
        domainMaps.put(dn.getUpgradeDomain(), domainValue);
      }
    }
    long endTime = Time.monotonicNow();

    dnMaps.entrySet().stream().sorted(Map.Entry.comparingByValue())
        .forEach(e -> System.out.println("DN: " + e.getKey()
            + ", Rack: " + dnRacks.get(e.getKey())
            + ", Domain: " + dnDomains.get(e.getKey())
            + ", times: " + e.getValue()));

    domainMaps.entrySet().stream().sorted(Map.Entry.comparingByValue())
        .forEach(e -> System.out.println("Domain: " + e.getKey() + ", value: " + e.getValue()));

    System.out.println("Total duration: " + (endTime - startTime));
  }

  private NetworkTopology initNetworkTopology(Configuration conf,
      String dfsHosts, String testDC) throws IOException {
    NetworkTopology networkTopology = DFSNetworkTopology.getInstance(conf);
    DatanodeAdminProperties[] datanodeAdminProperties = readFile(dfsHosts);
    for (DatanodeAdminProperties dnp : datanodeAdminProperties) {
      if (dnp.getRacketName().startsWith(testDC)) {
        DatanodeID datanodeID = new DatanodeID(dnp.getHostName(),
            dnp.getHostName(), "uuid-" + dnp.getHostName(),
            50010, 50020, 50030, 50040);
        DatanodeDescriptor dn = new DatanodeDescriptor(datanodeID, dnp.getRacketName());
        dn.setUpgradeDomain(dnp.getUpgradeDomain());
        DatanodeStorageInfo datanodeStorageInfo =
            dn.updateStorage(new DatanodeStorage("Fake-storage-ID-Ignored"));
        datanodeStorageInfo.setRemainingForTests(Integer.MAX_VALUE);
        assert networkTopology != null;
        networkTopology.add(dn);
        dnMaps.put(dn, 0);
        dnRacks.put(dn, dnp.getRacketName());
        dnDomains.put(dn, dnp.getUpgradeDomain());
      }
    }
    return networkTopology;
  }

  private DatanodeAdminProperties[] readFile(final String hostsFilePath)
      throws IOException {
    DatanodeAdminProperties[] allDNs = new DatanodeAdminProperties[0];
    ObjectMapper objectMapper = new ObjectMapper();
    File hostFile = new File(hostsFilePath);

    if (hostFile.length() > 0) {
      Reader input = new InputStreamReader(
          Files.newInputStream(hostFile.toPath()), StandardCharsets.UTF_8) ;
      allDNs = objectMapper.readValue(input, DatanodeAdminProperties[].class);
    }
    return allDNs;
  }

  public static class DatanodeAdminProperties {
    private String hostName;
    private String upgradeDomain;
    private String rackName;

    /**
     * Return the host name of the datanode.
     * @return the host name of the datanode.
     */
    public String getHostName() {
      return hostName;
    }

    /**
     * Set the host name of the datanode.
     * @param hostName the host name of the datanode.
     */
    public void setHostName(final String hostName) {
      this.hostName = hostName;
    }

    /**
     * Get the upgrade domain of the datanode.
     * @return the upgrade domain of the datanode.
     */
    public String getUpgradeDomain() {
      return upgradeDomain;
    }

    /**
     * Set the upgrade domain of the datanode.
     * @param upgradeDomain the upgrade domain of the datanode.
     */
    public void setUpgradeDomain(final String upgradeDomain) {
      this.upgradeDomain = upgradeDomain;
    }

    public String getRacketName() {
      return rackName;
    }

    public void setRackName(final String rackName) {
      this.rackName = rackName;
    }

    public String toString() {
      return String.format("hostName=%s, upgradeDomain=%s, rackName=%s",
          hostName, upgradeDomain, rackName);
    }
  }

  public static void main(String[] args) throws IOException {
    // the args[0] should be the dfs.include file contains the mapping from DN to upgrade domain.
    String inputPath = args[0];
    boolean forEC = Boolean.parseBoolean(args[1]);
    System.out.println("Start checking the balance of BlockPlacementPolicyDefault in rack mode");
    TestUpgradeDomainBlockPlacementPolicy1 defaultRackSTT =
        new TestUpgradeDomainBlockPlacementPolicy1(inputPath, false, "/STT");
    defaultRackSTT.chooseTargetNodeForNewBlocks(forEC);
    TestUpgradeDomainBlockPlacementPolicy1 defaultRackAT =
        new TestUpgradeDomainBlockPlacementPolicy1(inputPath, false, "/AirTrunk");
    defaultRackAT.chooseTargetNodeForNewBlocks(forEC);

    System.out.println("Start checking the balance of BlockPlacementPolicyDefault in domain mode");
    TestUpgradeDomainBlockPlacementPolicy1 defaultDomainSTT =
        new TestUpgradeDomainBlockPlacementPolicy1(inputPath, true, "/STT");
    defaultDomainSTT.chooseTargetNodeForNewBlocks(forEC);
    TestUpgradeDomainBlockPlacementPolicy1 defaultDomainAT =
        new TestUpgradeDomainBlockPlacementPolicy1(inputPath, true, "/AirTrunk");
    defaultDomainAT.chooseTargetNodeForNewBlocks(forEC);
  }
}
