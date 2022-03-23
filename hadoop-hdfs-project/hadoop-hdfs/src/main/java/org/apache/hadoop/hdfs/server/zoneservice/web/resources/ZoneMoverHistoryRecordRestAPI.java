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
package org.apache.hadoop.hdfs.server.zoneservice.web.resources;

import com.google.inject.Singleton;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.zoneservice.store.MigrationRecord;
import org.apache.hadoop.hdfs.server.zoneservice.store.Query;
import org.apache.hadoop.hdfs.server.zoneservice.store.SignalRecord;
import org.apache.hadoop.hdfs.server.zoneservice.store.StoreDriver;
import org.apache.hadoop.util.ReflectionUtils;
import org.codehaus.jackson.map.ObjectMapper;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import java.io.IOException;
import java.util.List;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ZONESERVICE_STORE_DRIVER_CLASS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ZONESERVICE_STORE_DRIVER_CLASS_DEFAULT;

@Singleton
@Path("migrationrecord/")
public class ZoneMoverHistoryRecordRestAPI {
  private final static Configuration conf = new Configuration();
  private final static String DEFAULT_MODE = "exact";

  public Class<? extends StoreDriver> driverClass = conf.getClass(
      DFS_ZONESERVICE_STORE_DRIVER_CLASS,
      DFS_ZONESERVICE_STORE_DRIVER_CLASS_DEFAULT,
      StoreDriver.class);
  private final StoreDriver driver = ReflectionUtils.newInstance(driverClass, conf);

  public ZoneMoverHistoryRecordRestAPI() {
    driver.init(conf, "historyServlet");
  }

  /**
   * Get all the signal record in the zk
   * @return List of the signal records
   */
  @GET
  @Path("signal/")
  public String getSignalRecord() {
    try {
      List<SignalRecord> signalRecordList =
          driver.getAll(SignalRecord.class).getRecords();
      return new
          ZoneServiceHttpResponse(Object2String(signalRecordList)).toString();
    } catch (IOException e) {
      e.printStackTrace();
      return new ZoneServiceHttpResponse(ResultCode.IO_EXCEPTION, "[]")
          .toString();
    }
  }

  /**
   * Get migration record by the given namespace&path
   * @param path      the path will be checked
   * @param nameSpace the namespace which migration is on
   * @param mode      recursive search or exact search
   * @return the migration record
   */
  @GET
  @Path("{path:.*}")
  public String getMigrationRecord(@PathParam("path") String path,
      @QueryParam("namespace") String nameSpace,
      @QueryParam("mode") @DefaultValue(DEFAULT_MODE) String mode) {
    return getHistoryRecord(nameSpace, path, mode);
  }

  private String getHistoryRecord(String nameSpace, String path, String mode) {
    try {
      if (mode.toLowerCase().equals("exact")) {
        MigrationRecord migrationRecord =
            new MigrationRecord(nameSpace, path, "");
        Query<MigrationRecord> query = new Query<>(migrationRecord);
        MigrationRecord result = driver.get(query, MigrationRecord.class);
        if (result == null) {
          return new ZoneServiceHttpResponse(ResultCode.NO_MIGRATION_RECORD, "[]")
              .toString();
        }
        return new ZoneServiceHttpResponse(Object2String(result)).toString();
      } else if (mode.toLowerCase().equals("recursive")) {
        MigrationRecord migrationRecord =
            new MigrationRecord(nameSpace, path, "");
        Query<MigrationRecord> query = new Query<>(migrationRecord);
        List<MigrationRecord> resultList =
            driver.getLike(query, MigrationRecord.class);
        if (resultList.isEmpty()) {
          return new
              ZoneServiceHttpResponse(ResultCode.NO_MIGRATION_RECORD, "[]")
              .toString();
        }
        return new ZoneServiceHttpResponse(Object2String(resultList)).toString();
      } else {
        return new ZoneServiceHttpResponse(ResultCode.IO_EXCEPTION, "[]")
            .toString();
      }
    } catch (IOException e) {
      e.printStackTrace();
      return new ZoneServiceHttpResponse(ResultCode.IO_EXCEPTION, "[]")
          .toString();
    }
  }

  /*
  Convert Object to Json String
   */
  private String Object2String(Object object) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.writeValueAsString(object);
  }
}
