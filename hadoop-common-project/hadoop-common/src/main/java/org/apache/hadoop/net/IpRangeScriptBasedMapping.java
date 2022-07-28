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

package org.apache.hadoop.net;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.NET_TOPOLOGY_IP_RANGE_DC_MAPPING_FILE_KEY;

public class IpRangeScriptBasedMapping extends ScriptBasedMapping {
  private static final Logger LOG =
      LoggerFactory.getLogger(IpRangeScriptBasedMapping.class);

  private static final int DEFAULT_MAK_BIT = 16;

  public IpRangeScriptBasedMapping() {
    super(new IpRangeMapping());
  }

  /**
   * Get the cached mapping and convert it to its real type
   * @return the inner raw script mapping.
   */
  private IpRangeMapping getRawMapping() {
    return (IpRangeMapping)rawMapping;
  }

  public synchronized void reloadIpRange2DC(Configuration conf) {
    String currentIpRange2DCFile = conf.get(NET_TOPOLOGY_IP_RANGE_DC_MAPPING_FILE_KEY, null);
    LOG.info("Will reload the ipRange2DC mapping with {}.", currentIpRange2DCFile);
    List<String> needReloadList = getRawMapping().reloadIpRange(currentIpRange2DCFile);
    super.reloadCachedMappings(needReloadList);
    LOG.info("Successfully reload the ipRange2DC mapping and remove {} ips from cache.",
        needReloadList.size());
  }

  protected static class IpRangeMapping extends RawScriptBasedMapping {
    private volatile String ipRange2DCFile = null;
    private final Set<String> cachedIps = new HashSet<>();
    private Map<Pair<String, Integer>, String> ipRangeDCMapping = null;

    /**
     * Constructor. The mapping is not ready to use until
     * {@link #setConf(Configuration)} has been called
     */
    public IpRangeMapping() {}

    /**
     * Set the configuration and extract the configuration parameters of interest
     * @param conf the new configuration
     */
    @Override
    public void setConf(Configuration conf) {
      super.setConf(conf);
      ipRange2DCFile = getConf().get(NET_TOPOLOGY_IP_RANGE_DC_MAPPING_FILE_KEY, null);
      ipRangeDCMapping = load(ipRange2DCFile);
    }

    @Override
    public List<String> resolve(List<String> names) {
      List<String> superResult = super.resolve(names);
      for (int index = 0; index < names.size(); index++) {
        String network = superResult.get(index);
        // Can not get the actual network.
        if (network.contains(NetworkTopology.DEFAULT_RACK)) {
          // Try to get the DC from ipRang2DC mapping.
          String name = names.get(index);
          String rangeOut = resolveTopologyFromIpSegment(name);
          LOG.debug("Resolve {} from ipRangeDC mapping and result is {}.", name, rangeOut);
          if (rangeOut != null) {
            superResult.set(index, rangeOut);
            cachedIps.add(name);
          } else {
            LOG.warn("Cannot resolve {} from ipRangeDC mapping, please confirm.", name);
          }
        }
      }
      return superResult;
    }

    private String resolveTopologyFromIpSegment(String ip) {
      for (Map.Entry<Pair<String, Integer>, String> entry : ipRangeDCMapping.entrySet()) {
        String ipSegment = entry.getKey().getLeft();
        int maskBit = entry.getKey().getRight();
        String dcInfo = entry.getValue();

        String currentIpSegment = NetUtils.getIpSegment(ip, maskBit);
        if (currentIpSegment.equals(ipSegment)) {
          return "/" + dcInfo + NetworkTopology.DEFAULT_RACK;
        }
      }
      return null;
    }

    private Map<Pair<String, Integer>, String> load(String filename) {
      Map<Pair<String, Integer>, String> loadMap = new HashMap<>();
      if (StringUtils.isBlank(filename)) {
        LOG.warn(NET_TOPOLOGY_IP_RANGE_DC_MAPPING_FILE_KEY + " not configured. ");
        return null;
      }

      try (BufferedReader reader = new BufferedReader(new InputStreamReader(
          Files.newInputStream(Paths.get(filename)), StandardCharsets.UTF_8))) {
        String line = reader.readLine();
        while (line != null) {
          line = line.trim();
          if (line.length() != 0 && line.charAt(0) != '#') {
            String[] columns = line.split(",");
            if (columns.length == 2) {
              String ipRange = columns[0];
              String[] ipColumns = ipRange.split("/");
              String ipSegment = ipColumns[0];
              int maskBit = DEFAULT_MAK_BIT;
              if (ipColumns.length == 2) {
                try {
                  maskBit = Integer.parseInt(ipColumns[1]);
                } catch (NumberFormatException e1) {
                  LOG.warn("Found unformatted ip range {} and will use default mask bit {}.",
                      line, DEFAULT_MAK_BIT, e1);
                }
              } else {
                LOG.warn("Found unformatted ip range {} and will use default mask bit {}.",
                    line, DEFAULT_MAK_BIT);
              }
              String dcName = columns[1];
              loadMap.put(Pair.of(ipSegment, maskBit), dcName);
            } else {
              LOG.warn("Line does not have two columns. Ignoring. " + line);
            }
          }
          line = reader.readLine();
        }
      } catch (Exception e) {
        LOG.warn(filename + " cannot be read.", e);
        return null;
      }
      return loadMap;
    }

    public List<String> reloadIpRange(String newIpRange2DCFile) {
      this.ipRangeDCMapping = load(newIpRange2DCFile);
      this.ipRange2DCFile = newIpRange2DCFile;
      getConf().set(NET_TOPOLOGY_IP_RANGE_DC_MAPPING_FILE_KEY, newIpRange2DCFile);
      List<String> needReloadCacheList = new ArrayList<>(this.cachedIps);
      this.cachedIps.clear();
      return needReloadCacheList;
    }


    @Override
    public String toString() {
      return super.toString() + ", ipRange2DC file is:" + this.ipRange2DCFile;
    }
  }
}
