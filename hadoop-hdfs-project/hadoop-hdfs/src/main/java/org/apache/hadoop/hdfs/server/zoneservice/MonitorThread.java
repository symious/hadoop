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
package org.apache.hadoop.hdfs.server.zoneservice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.zoneservice.metrics.ZoneServiceMetrics;
import org.apache.hadoop.hdfs.server.zoneservice.store.MigrationRecord;
import org.apache.hadoop.hdfs.server.zoneservice.store.Query;
import org.apache.hadoop.hdfs.server.zoneservice.store.SignalRecord;
import org.apache.hadoop.hdfs.server.zoneservice.store.StoreDriver;
import org.apache.hadoop.hdfs.server.zoneservice.web.resources.ResultCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Date;

public class MonitorThread extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(
      MonitorThread.class);

  private final Configuration conf;
  private final URI nameSpace;
  private ZoneServiceMetrics metrics;
  private final StoreDriver driver;
  private final SignalRecord signalRecord;

  public MonitorThread(String name, Configuration conf, URI nameSpace, StoreDriver driver,
      SignalRecord signalRecord) {
    super(name);
    this.conf = conf;
    this.nameSpace = nameSpace;
    this.driver = driver;
    this.signalRecord = signalRecord;
    metrics = ZoneService.getMetrics();
  }

  public Configuration getConf() {
    return conf;
  }

  public URI getNameSpace() {
    return nameSpace;
  }

  public void run() {
    LOG.info("start run monitor thread for {}", this.getName());
    metrics.startMonitorThread();
    Date startTime = new Date();
    try {
      ZoneMover.run(conf, nameSpace, true);
    } catch (IOException e) {
      e.printStackTrace();
      AuditLogger.logRuleProcess("monitorThread", nameSpace.getAuthority(),
          "", "", startTime, new Date(),
          ResultCode.IO_EXCEPTION.getMsg(), "monitor");
    } finally {
      LOG.info("stop run monitor thread for {}", this.getName());
      try {
        driver.remove(new Query<>(signalRecord), SignalRecord.class);
      } catch (IOException e) {
        AuditLogger.logRuleProcess(
            "RemoveSignalRecord", nameSpace.getAuthority(), "", "",
            startTime, new Date(), ResultCode.IO_EXCEPTION.getMsg(), "");
        LOG.error("Remove zk signal record for {} fail.", nameSpace.getAuthority(), e);
      }
      metrics.stopMonitorThread();
    }
  }
}
