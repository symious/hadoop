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

package org.apache.hadoop.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SysInfoMac extends SysInfo {
  private static final Logger LOG = LoggerFactory.getLogger(SysInfoMac.class);

  // top command result map key.
  private static final String LOAD1 = "load1";
  private static final String LOAD5 = "load5";
  private static final String CPU_USAGE_USER = "cpuUsageUser";
  private static final String CPU_USAGE_SYSTEM = "cpuUsageSystem";
  private static final String CPU_USAGE_IDLE = "cpuUSageIdle";
  private static final String NETWORKS_READ = "networksRead";
  private static final String NETWORKS_WRITE = "networksWrite";
  private static final String DISTS_READ = "distsRead";
  private static final String DISTS_WRITE = "distsWrite";
  private static final String UPTIME = "uptime";

  // Configuration of sysctl command.
  private static final String MACHDEP_CPU_CORE_COUNT_STRING =
      "machdep.cpu.core_count";
  private static final String HW_CPUFREQUENCY_STRING = "hw.cpufrequency";
  private static final String MACHDEP_CPU_THERMAL_SENSOR_STRING =
      "machdep.cpu.thermal.sensor";
  private static final String VM_SWAPUSAGE_STRING = "vm.swapusage";
  private static final String HW_MEMSIZE_STRING = "hw.memsize";

  // Filter out the lines that need to be parsed
  private static final Set<String> sysctlKeySet = new HashSet<>(Arrays
      .asList(MACHDEP_CPU_CORE_COUNT_STRING, HW_CPUFREQUENCY_STRING,
          MACHDEP_CPU_THERMAL_SENSOR_STRING, VM_SWAPUSAGE_STRING,
          HW_MEMSIZE_STRING));

  // Configuration of vm_stat command
  private static final String PAGES_FREE_STRING = "Pages free";
  private static final String PAGES_INACTIVE_STRING = "Pages inactive";

  private static final Set<String> vmStatKeySet =
      new HashSet<>(Arrays.asList(PAGES_FREE_STRING, PAGES_INACTIVE_STRING));

  // Mach Virtual Memory Statistics: (page size of 4096 bytes)
  private static final long pageSize = 4096L;

  /*
   * The regex used to parse the value of "vm.swapusage", for example:
   * vm.swapusage: total = 2048.00M  used = 1628.75M  free = 419.25M  (encrypted)
   */
  private final Pattern swapUsagePattern = Pattern.compile(
      "^total[ \t]*=[ \t]*([0-9.]*)M[ \t]*"
          + "used[ \t]*=[ \t]*([0-9.]*)M[ \t]*"
          + "free[ \t]*=[ \t]*([0-9.]*)M.*");

  private Map<String, String> sysctlMap;
  private Map<String, String> vmStatMap;

  /*
   * The total and free value of "vm.swapusage" configuration
   * vm.swapusage: total = 2048.00M  used = 1628.75M  free = 419.25M  (encrypted)
   */
  private float swapTotal = 0; // MB
  private float swapFree = 0; // MB

  // The average load of the system within 1 minute, 5 minutes.
  private float loadAvg1 = 0;
  private float loadAvg5 = 0;

  private long networksRead = 0; // bytes
  private long networksWrite = 0;
  private long distsRead = 0;
  private long distsWrite = 0;

  private float cpuUsageUser = -1; // 0-100%
  private float cpuUsageSystem = -1;

  // How long a system has been “up” and running without a shut down or restart
  private long cpuUptime = -1; // ms

  /*
   * Whether the sysctl command has been executed.
   * The statically configured value can be reused,
   * no need to execute the command twice.
   */
  private boolean execSysctl = false;

  /**
   * Execute the "sysctl -a" command and use KVParser to parse the result.
   *
   * @param execSysctlAgain If need to execute again. The statically configured
   *                        value can be reused, no need to execute the command
   *                        twice.
   */
  private void execSysctlCommand(boolean execSysctlAgain) {
    if (execSysctl && !execSysctlAgain) {
      return;
    }
    List<String> lineList = execCommand("sysctl -a");
    this.sysctlMap = new KVParser(sysctlKeySet).parse(lineList);
    this.swapUsageStat();
    this.execSysctl = true;
  }

  private void execVmStatCommand() {
    this.vmStatMap =
        new KVParser(vmStatKeySet).parse(execCommand("vm_stat"));

    Set<Map.Entry<String, String>> entries = this.vmStatMap.entrySet();
    for (Map.Entry<String, String> e : entries) {
      if (e.getValue().endsWith(".")) {
        this.vmStatMap.put(e.getKey(),
            e.getValue().substring(0, e.getValue().length() - 1));
      }
    }
  }

  private void execTopCommand() {
    Map<String, String> topMap = new TopParser().parse(execCommand("top -l 1"));
    this.networksRead = convertToBytes(topMap.get(NETWORKS_READ));
    this.networksWrite = convertToBytes(topMap.get(NETWORKS_WRITE));
    this.distsRead = convertToBytes(topMap.get(DISTS_READ));
    this.distsWrite = convertToBytes(topMap.get(DISTS_WRITE));
    this.loadAvg1 =
        topMap.containsKey(LOAD1) ? Float.parseFloat(topMap.get(LOAD1)) :
            this.loadAvg1;
    this.loadAvg5 =
        topMap.containsKey(LOAD5) ? Float.parseFloat(topMap.get(LOAD5)) :
            this.loadAvg5;
    this.cpuUsageUser = topMap.containsKey(CPU_USAGE_USER) ?
        Float.parseFloat(topMap.get(CPU_USAGE_USER)) : this.cpuUsageUser;
    this.cpuUsageSystem = topMap.containsKey(CPU_USAGE_SYSTEM) ?
        Float.parseFloat(topMap.get(CPU_USAGE_SYSTEM)) : this.cpuUsageSystem;
  }

  private void execUptimeCommand() {
    Map<String, String> map = new UptimeParser().parse(execCommand("uptime"));
    if (map.containsKey(UPTIME)) {
      this.cpuUptime = Long.parseLong(map.get(UPTIME));
    }
  }

  @VisibleForTesting
  protected List<String> execCommand(String command) {
    BufferedReader reader = null;
    List<String> lineList = new ArrayList<>();
    try {
      Process process = Runtime.getRuntime().exec(command);
      reader =
          new BufferedReader(new InputStreamReader(process.getInputStream()));
      String line;
      while ((line = reader.readLine()) != null) {
        lineList.add(line);
      }
    } catch (IOException io) {
      LOG.warn("Error reading the stream " + io);
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (Exception in) {
          LOG.warn("Error close the stream " + in);
        }
      }
    }
    return lineList;
  }

  private long convertToBytes(String s) {
    long cnt = 0;
    if (!StringUtils.isNullOrEmpty(s)) {
      String numStr = s.substring(0, s.length() - 1);
      String upperStr = s.toUpperCase(Locale.ENGLISH);
      if (upperStr.endsWith("G")) {
        cnt = Float.valueOf(Float.parseFloat(numStr) * 1024 * 1024 * 1024)
            .longValue();
      } else if (upperStr.endsWith("M")) {
        cnt = Float.valueOf(Float.parseFloat(numStr) * 1024 * 1024).longValue();
      } else if (upperStr.endsWith("K")) {
        cnt = Float.valueOf(Float.parseFloat(numStr) * 1024).longValue();
      } else if (upperStr.endsWith("B")) {
        cnt = Float.valueOf(numStr).longValue();
      }
    }
    return cnt;
  }

  private void swapUsageStat() {
    String swapUsage = this.sysctlMap.get(VM_SWAPUSAGE_STRING);
    Matcher matcher = swapUsagePattern.matcher(swapUsage);
    if (matcher.find()) {
      this.swapTotal = Float.parseFloat(matcher.group(1));
      this.swapFree = Float.parseFloat(matcher.group(3));
    }
  }

  @Override
  public long getVirtualMemorySize() {
    this.execSysctlCommand(true);
    return this.getPhysicalMemorySize() + Float
        .valueOf(this.swapTotal * 1024 * 1024).longValue();
  }

  @Override
  public long getPhysicalMemorySize() {
    this.execSysctlCommand(false);
    return this.sysctlMap.containsKey(HW_MEMSIZE_STRING) ?
        Long.parseLong(this.sysctlMap.get(HW_MEMSIZE_STRING)) : 0;
  }

  @Override
  public long getAvailableVirtualMemorySize() {
    this.execSysctlCommand(true);
    return this.getAvailablePhysicalMemorySize() + Float
        .valueOf(swapFree * 1024 * 1024).longValue();
  }

  @Override
  public long getAvailablePhysicalMemorySize() {
    this.execVmStatCommand();
    long pageFree = vmStatMap.containsKey(PAGES_FREE_STRING) ?
        Long.parseLong(vmStatMap.get(PAGES_FREE_STRING)) : 0;
    long pageInactive = vmStatMap.containsKey(PAGES_INACTIVE_STRING) ?
        Long.parseLong(vmStatMap.get(PAGES_INACTIVE_STRING)) : 0;
    return pageFree * pageSize + pageInactive * pageSize;
  }

  @Override
  public int getNumProcessors() {
    this.execSysctlCommand(false);
    return this.sysctlMap.containsKey(MACHDEP_CPU_THERMAL_SENSOR_STRING) ?
        Integer
            .parseInt(this.sysctlMap.get(MACHDEP_CPU_THERMAL_SENSOR_STRING)) :
        0;
  }

  @Override
  public int getNumCores() {
    this.execSysctlCommand(false);
    return this.sysctlMap.containsKey(MACHDEP_CPU_CORE_COUNT_STRING) ?
        Integer.parseInt(this.sysctlMap.get(MACHDEP_CPU_CORE_COUNT_STRING)) : 0;
  }

  @Override
  public long getCpuFrequency() {
    this.execSysctlCommand(false);
    return this.sysctlMap.containsKey(HW_CPUFREQUENCY_STRING) ?
        Long.parseLong(this.sysctlMap.get(HW_CPUFREQUENCY_STRING)) : 0;
  }

  @Override
  public long getCumulativeCpuTime() {

    float cpuUsagePer = getCpuUsagePercentage();
    this.execUptimeCommand();
    if (cpuUsagePer == -1 || this.cpuUptime == -1) {
      return 0;
    }
    return Float.valueOf(getCpuUsagePercentage() * this.cpuUptime / 100)
        .longValue();
  }

  @Override
  public float getCpuUsagePercentage() {
    this.execTopCommand();
    if (this.cpuUsageUser == -1 && this.cpuUsageSystem == -1) {
      return -1;
    }
    float user = this.cpuUsageUser == -1 ? 0 : this.cpuUsageUser;
    float system = this.cpuUsageSystem == -1 ? 0 : this.cpuUsageSystem;
    return (user + system) / this.getNumProcessors();
  }

  @Override
  public float getLoad1() {
    this.execTopCommand();
    return this.loadAvg1;
  }

  @Override
  public float getLoad5() {
    this.execTopCommand();
    return this.loadAvg5;
  }

  @Override
  public float getNumVCoresUsed() {
    float cpuUsagePer = this.getCpuUsagePercentage();
    if (cpuUsagePer == -1) {
      return -1;
    }
    return cpuUsagePer / 100F;
  }

  @Override
  public long getNetworkBytesRead() {
    this.execTopCommand();
    return this.networksRead;
  }

  @Override
  public long getNetworkBytesWritten() {
    this.execTopCommand();
    return this.networksWrite;
  }

  @Override
  public long getStorageBytesRead() {
    this.execTopCommand();
    return this.distsRead;
  }

  @Override
  public long getStorageBytesWritten() {
    this.execTopCommand();
    return this.distsWrite;
  }

  interface CommandResultParser {
    Map<String, String> parse(List<String> lineList);
  }

  static class KVParser implements CommandResultParser {

    private final Set<String> keySet;

    public KVParser(Set<String> keySet) {
      this.keySet = keySet;
    }

    @Override
    public Map<String, String> parse(List<String> lineList) {
      Map<String, String> map = new HashMap<>();
      for (String line : lineList) {
        int idx = line.indexOf(":");
        if (idx > 0 && idx < line.length() - 1) {
          String key = line.substring(0, idx).trim();
          if (this.keySet.contains(key)) {
            String value = line.substring(idx + 1).trim();
            map.put(key, value);
          }
        }
      }
      return map;
    }
  }

  static class TopParser implements CommandResultParser {

    private Pattern loadAvgPattern = Pattern
        .compile("^Load Avg:[ \t]*([0-9.]+),[ \t]*([0-9.]+),[ \t]*([0-9.]+)");
    private Pattern cpuUsagePattern = Pattern.compile(
        "^CPU usage:[ \t]*([0-9.]+)%[ \t]*user,"
            + "[ \t]*([0-9.]+)%[ \t]*sys,[ \t]*([0-9.]+)%[ \t]*idle");
    private Pattern networksPattern = Pattern.compile(
        "^Networks: packets:[ \t]*[0-9]*/([0-9.]+[GMKB])[ \t]*in,"
            + "[ \t]*[0-9]*/([0-9.]+[GMKB])[ \t]*out.*");
    private Pattern distsPattern = Pattern.compile(
        "^Disks:[ \t]*[0-9]*/([0-9.]+[GMKB])[ \t]*read,"
            + "[ \t]*[0-9]*/([0-9.]+[GMKB])[ \t]*written.*");

    @Override
    public Map<String, String> parse(List<String> lineList) {
      Map<String, String> map = new HashMap<>();

      for (String line : lineList) {
        if (line.startsWith("Load Avg")) {
          Matcher matcher = loadAvgPattern.matcher(line);
          if (matcher.find()) {
            map.put(LOAD1, matcher.group(1));
            map.put(LOAD5, matcher.group(2));
          }
        } else if (line.startsWith("CPU usage")) {
          Matcher matcher = cpuUsagePattern.matcher(line);
          if (matcher.find()) {
            map.put(CPU_USAGE_USER, matcher.group(1));
            map.put(CPU_USAGE_SYSTEM, matcher.group(2));
            map.put(CPU_USAGE_IDLE, matcher.group(3));
          }
        } else if (line.startsWith("Networks")) {
          Matcher matcher = networksPattern.matcher(line);
          if (matcher.find()) {
            map.put(NETWORKS_READ,
                matcher.group(1));
            map.put(NETWORKS_WRITE,
                matcher.group(2));
          }
        } else if (line.startsWith("Disks")) {
          Matcher matcher = distsPattern.matcher(line);
          if (matcher.find()) {
            map.put(DISTS_READ, matcher.group(1));
            map.put(DISTS_WRITE, matcher.group(2));
          }
        }
      }
      return map;
    }
  }

  static class UptimeParser implements CommandResultParser {

    private final Pattern uptimePattern = Pattern.compile(
        "up[ \t]*(([\\d]*)[ \t]*(days|mins),)?([ \t]*([\\d]*):([\\d]*))?");

    @Override
    public Map<String, String> parse(List<String> lineList) {
      Map<String, String> map = new HashMap<>();
      //15:49  up 5 days, 19:42, 12 users, load averages: 2.02 3.87 3.73
      for (String line : lineList) {
        Matcher m = uptimePattern.matcher(line);
        if (m.find()) {
          int days = 0, hours = 0, mins = 0;
          if (m.group(2) != null) {
            int num = Integer.parseInt(m.group(2));
            String unit = m.group(3);
            switch (unit) {
              case "days" : days = num; break;
              case "mins" : mins = num; break;
            }
          }
          hours =
              m.group(5) != null ? hours + Integer.parseInt(m.group(5)) : hours;
          mins =
              m.group(6) != null ? mins + Integer.parseInt(m.group(6)) : mins;
          map.put(UPTIME, String.valueOf(
              days * 24L * 3600 * 1000 + hours * 3600L * 1000
                  + mins * 60L * 1000));
        }
      }
      return map;
    }
  }

}
