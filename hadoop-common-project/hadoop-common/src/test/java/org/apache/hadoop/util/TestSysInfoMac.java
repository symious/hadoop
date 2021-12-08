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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

public class TestSysInfoMac {

  @Test
  public void testVmStatParser() {
    SysInfoMac sysInfoMac = new SysInfoMac();
    SysInfoMac mockSysInfoMac = Mockito.spy(sysInfoMac);
    List<String> mockVmStat = Arrays.asList(
        "Pages free:                                7416.\n",
        "Pages inactive:                         1351509.\n");

    when(mockSysInfoMac.execCommand("vm_stat")).thenReturn(mockVmStat);

    long pageSize = 4096L;
    long availablePhysicalMemory = 7416 * pageSize + 1351509 * pageSize;
    assertEquals(mockSysInfoMac.getAvailablePhysicalMemorySize(),
        availablePhysicalMemory);
  }

  @Test
  public void testSysctlParser() {
    SysInfoMac sysInfoMac = new SysInfoMac();
    SysInfoMac mockSysInfoMac = Mockito.spy(sysInfoMac);

    List<String> lineList = Arrays.asList(
        "Pages free:                                7416.\n",
        "Pages inactive:                         1351509.\n");

    List<String> mockSysctl = Arrays
        .asList("machdep.cpu.core_count: 8\n", "hw.cpufrequency: 2300000000\n",
            "machdep.cpu.thermal.sensor: 1\n",
            "vm.swapusage: total = 7168.00M  used = 6178.25M  free = 989.75M  (encrypted)\n",
            "hw.memsize: 17179869184\n");

    when(mockSysInfoMac.execCommand("vm_stat")).thenReturn(lineList);
    when(mockSysInfoMac.execCommand("sysctl -a")).thenReturn(mockSysctl);

    long pageSize = 4096L;
    long availablePhysicalMemory = 7416 * pageSize + 1351509 * pageSize;

    assertEquals(mockSysInfoMac.getAvailableVirtualMemorySize(),
        availablePhysicalMemory + Float.valueOf(989.75F * 1024 * 1024)
            .longValue());

    assertEquals(mockSysInfoMac.getVirtualMemorySize(),
        17179869184L + Float.valueOf(7168.00F * 1024 * 1024).longValue());
    assertEquals(mockSysInfoMac.getPhysicalMemorySize(), 17179869184L);

    assertEquals(mockSysInfoMac.getNumProcessors(), 1);
    assertEquals(mockSysInfoMac.getNumCores(), 8);
    assertEquals(mockSysInfoMac.getCpuFrequency(), 2300000000L);
  }

  @Test
  public void testTopParser() {

    SysInfoMac sysInfoMac = new SysInfoMac();
    SysInfoMac mockSysInfoMac = Mockito.spy(sysInfoMac);

    List<String> mockTop = Arrays
        .asList("Processes: 724 total, 2 running, 722 sleeping, 4007 threads \n",
            "2021/09/09 20:01:24\n",
            "Load Avg: 3.04, 2.63, 2.86 \n",
            "CPU usage: 8.52% user, 9.27% sys, 82.19% idle \n",
            "SharedLibs: 338M resident, 44M data, 87M linkedit.\n",
            "MemRegions: 278085 total, 6324M resident, 101M private, 2291M shared.\n",
            "PhysMem: 16G used (4284M wired), 96M unused.\n",
            "VM: 5504G vsize, 2321M framework vsize, 95572221(0) swapins, 99131454(0) swapouts.\n",
            "Networks: packets: 7675420/27G in, 8143149/25G out.\n",
            "Disks: 10461906/517G read, 5668540/542G written.\n");

    List<String> mockSysctl = Arrays
        .asList("machdep.cpu.core_count: 8\n", "hw.cpufrequency: 2300000000\n",
            "machdep.cpu.thermal.sensor: 1\n",
            "vm.swapusage: total = 7168.00M  used = 6178.25M  free = 989.75M  (encrypted)\n",
            "hw.memsize: 17179869184\n");

    when(mockSysInfoMac.execCommand("top -l 1")).thenReturn(mockTop);
    when(mockSysInfoMac.execCommand("uptime")).thenReturn(Collections
        .singletonList(
            "20:03  up 5 days, 23:56, 12 users, load averages: 3.34 2.80 2.89\n"));
    when(mockSysInfoMac.execCommand("sysctl -a")).thenReturn(mockSysctl);

    long uptime = 5 * 24L * 3600 * 1000 + 23 * 3600L * 1000 + 56 * 60L * 1000;
    assertEquals(mockSysInfoMac.getCumulativeCpuTime(),
        Float.valueOf((8.52F + 9.27F) * uptime / 100F).longValue());

    assertEquals(mockSysInfoMac.getCpuUsagePercentage(), 8.52F + 9.27F, 0.001F);

    assertEquals(mockSysInfoMac.getLoad1(), 3.04F, 0.001);
    assertEquals(mockSysInfoMac.getLoad5(), 2.63F, 0.001);
    assertEquals(mockSysInfoMac.getNumVCoresUsed(), (8.52F + 9.27F) / 100,
        0.00001);
    assertEquals(mockSysInfoMac.getNetworkBytesRead(),
        27L * 1024 * 1024 * 1024);
    assertEquals(mockSysInfoMac.getNetworkBytesWritten(),
        25L * 1024 * 1024 * 1024);
    assertEquals(mockSysInfoMac.getStorageBytesRead(),
        517L * 1024 * 1024 * 1024);
    assertEquals(mockSysInfoMac.getStorageBytesWritten(),
        542L * 1024 * 1024 * 1024);
  }

  @Test
  public void testTopParserMegabyteParser() {
    SysInfoMac sysInfoMac = new SysInfoMac();
    SysInfoMac mockSysInfoMac = Mockito.spy(sysInfoMac);

    List<String> mockTop = Arrays
        .asList("Processes: 724 total, 2 running, 722 sleeping, 4007 threads \n",
            "2021/09/09 20:01:24\n",
            "Load Avg: 3.04, 2.63, 2.86 \n",
            "CPU usage: 8.52% user, 9.27% sys, 82.19% idle \n",
            "SharedLibs: 338M resident, 44M data, 87M linkedit.\n",
            "MemRegions: 278085 total, 6324M resident, 101M private, 2291M shared.\n",
            "PhysMem: 16G used (4284M wired), 96M unused.\n",
            "VM: 5504G vsize, 2321M framework vsize, 95572221(0) swapins, 99131454(0) swapouts.\n",
            "Networks: packets: 7675420/27M in, 8143149/25M out.\n",
            "Disks: 10461906/517M read, 5668540/542M written.\n");

    when(mockSysInfoMac.execCommand("top -l 1")).thenReturn(mockTop);

    assertEquals(mockSysInfoMac.getNetworkBytesRead(),
        27L * 1024 * 1024);
    assertEquals(mockSysInfoMac.getNetworkBytesWritten(),
        25L * 1024 * 1024);
    assertEquals(mockSysInfoMac.getStorageBytesRead(),
        517L * 1024 * 1024);
    assertEquals(mockSysInfoMac.getStorageBytesWritten(),
        542L * 1024 * 1024);
  }

  @Test
  public void testUptimeMinsParser() {
    SysInfoMac sysInfoMac = new SysInfoMac();
    SysInfoMac mockSysInfoMac = Mockito.spy(sysInfoMac);

    List<String> mockTop = Arrays
        .asList("Processes: 724 total, 2 running, 722 sleeping, 4007 threads \n",
            "2021/09/09 20:01:24\n",
            "Load Avg: 3.04, 2.63, 2.86 \n",
            "CPU usage: 8.52% user, 9.27% sys, 82.19% idle \n",
            "SharedLibs: 338M resident, 44M data, 87M linkedit.\n",
            "MemRegions: 278085 total, 6324M resident, 101M private, 2291M shared.\n",
            "PhysMem: 16G used (4284M wired), 96M unused.\n",
            "VM: 5504G vsize, 2321M framework vsize, 95572221(0) swapins, 99131454(0) swapouts.\n",
            "Networks: packets: 7675420/27G in, 8143149/25G out.\n",
            "Disks: 10461906/517G read, 5668540/542G written.\n");

    List<String> mockSysctl = Arrays
        .asList("machdep.cpu.core_count: 8\n", "hw.cpufrequency: 2300000000\n",
            "machdep.cpu.thermal.sensor: 1\n",
            "vm.swapusage: total = 7168.00M  used = 6178.25M  free = 989.75M  (encrypted)\n",
            "hw.memsize: 17179869184\n");

    when(mockSysInfoMac.execCommand("uptime")).thenReturn(Collections
        .singletonList(
            "10:29  up 2 mins, 2 users, load averages: 4.89 2.58 1.07\n"));

    when(mockSysInfoMac.execCommand("top -l 1")).thenReturn(mockTop);

    when(mockSysInfoMac.execCommand("sysctl -a")).thenReturn(mockSysctl);

    long uptime = 2 * 60L * 1000;
    assertEquals(mockSysInfoMac.getCumulativeCpuTime(),
        Float.valueOf((8.52F + 9.27F) * uptime / 100F).longValue());
  }

  @Test
  public void testUptimeHoursParser() {
    SysInfoMac sysInfoMac = new SysInfoMac();
    SysInfoMac mockSysInfoMac = Mockito.spy(sysInfoMac);

    List<String> mockTop = Arrays
        .asList("Processes: 724 total, 2 running, 722 sleeping, 4007 threads \n",
            "2021/09/09 20:01:24\n",
            "Load Avg: 3.04, 2.63, 2.86 \n",
            "CPU usage: 8.52% user, 9.27% sys, 82.19% idle \n",
            "SharedLibs: 338M resident, 44M data, 87M linkedit.\n",
            "MemRegions: 278085 total, 6324M resident, 101M private, 2291M shared.\n",
            "PhysMem: 16G used (4284M wired), 96M unused.\n",
            "VM: 5504G vsize, 2321M framework vsize, 95572221(0) swapins, 99131454(0) swapouts.\n",
            "Networks: packets: 7675420/27G in, 8143149/25G out.\n",
            "Disks: 10461906/517G read, 5668540/542G written.\n");

    List<String> mockSysctl = Arrays
        .asList("machdep.cpu.core_count: 8\n", "hw.cpufrequency: 2300000000\n",
            "machdep.cpu.thermal.sensor: 1\n",
            "vm.swapusage: total = 7168.00M  used = 6178.25M  free = 989.75M  (encrypted)\n",
            "hw.memsize: 17179869184\n");

    when(mockSysInfoMac.execCommand("uptime")).thenReturn(Collections
        .singletonList(
            "11:27  up  1:01, 3 users, load averages: 2.18 2.35 2.38\n"));
    when(mockSysInfoMac.execCommand("top -l 1")).thenReturn(mockTop);

    when(mockSysInfoMac.execCommand("sysctl -a")).thenReturn(mockSysctl);

    long uptime = 3600L * 1000 + 60L * 1000;
    assertEquals(mockSysInfoMac.getCumulativeCpuTime(),
        Float.valueOf((8.52F + 9.27F) * uptime / 100F).longValue());
  }
}
