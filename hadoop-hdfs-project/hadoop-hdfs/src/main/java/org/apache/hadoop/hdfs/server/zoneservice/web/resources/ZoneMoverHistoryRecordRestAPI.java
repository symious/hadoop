package org.apache.hadoop.hdfs.server.zoneservice.web.resources;

import com.google.inject.Singleton;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.zoneservice.store.BaseRecord;
import org.apache.hadoop.hdfs.server.zoneservice.store.MigrationRecord;
import org.apache.hadoop.hdfs.server.zoneservice.store.Query;
import org.apache.hadoop.hdfs.server.zoneservice.store.StoreDriver;
import org.apache.hadoop.util.ReflectionUtils;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import java.io.IOException;

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
    try {
      return getHistoryRecord(nameSpace, path, mode);
    } catch (NullPointerException e) {
      return ResultCode.NO_MIGRATION_RECORD.toString();
    }
  }

  private String getHistoryRecord(String nameSpace, String path, String mode)
      throws NullPointerException{
    try {
      if (mode.toLowerCase().equals("exact")) {
        MigrationRecord migrationRecord =
            new MigrationRecord(nameSpace, path, "");
        Query<BaseRecord> query = new Query<BaseRecord>(migrationRecord);
        return driver.get(query, BaseRecord.class).toString();
      } else if (mode.toLowerCase().equals("recursive")) {
        MigrationRecord migrationRecord =
            new MigrationRecord(nameSpace, path, "");
        Query<BaseRecord> query = new Query<BaseRecord>(migrationRecord);
        return driver.getLike(query, BaseRecord.class).toString();
      } else { return ResultCode.IO_EXCEPTION.toString(); }
    } catch (IOException e) {
      return ResultCode.IO_EXCEPTION.toString();
    }
  }
}
