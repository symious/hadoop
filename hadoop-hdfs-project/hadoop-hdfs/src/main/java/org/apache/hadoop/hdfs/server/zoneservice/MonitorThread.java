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
import org.apache.hadoop.hdfs.server.zoneservice.web.resources.ResultCode;

import java.io.IOException;
import java.net.URI;
import java.util.Date;

public class MonitorThread extends Thread {
  private final Configuration conf;
  private final URI nameSpace;

  public MonitorThread(String name, Configuration conf, URI nameSpace) {
    super(name);
    this.conf = conf;
    this.nameSpace = nameSpace;
  }

  public Configuration getConf() {
    return conf;
  }

  public URI getNameSpace() {
    return nameSpace;
  }

  public void run() {
    Date startTime = new Date();
    try {
      ZoneMover.run(conf, nameSpace, true);
    } catch (IOException e) {
      e.printStackTrace();
      AuditLogger.logRuleProcess("monitorThread", nameSpace.getAuthority(),
          "", "", startTime, new Date(),
          ResultCode.IO_EXCEPTION.getMsg(), "monitor");
    }
  }
}
