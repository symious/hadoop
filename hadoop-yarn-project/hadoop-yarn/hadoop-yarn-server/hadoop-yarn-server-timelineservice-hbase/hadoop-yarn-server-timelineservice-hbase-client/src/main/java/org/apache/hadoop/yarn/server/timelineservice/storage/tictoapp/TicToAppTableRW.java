package org.apache.hadoop.yarn.server.timelineservice.storage.tictoapp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.BaseTableRW;

import java.io.IOException;

public class TicToAppTableRW extends BaseTableRW<TicToAppTable> {
  /** tic_to_app prefix. */
  private static final String PREFIX =
      YarnConfiguration.TIMELINE_SERVICE_PREFIX + "tic-to-app";

  /** config param name that specifies the domain table name. */
  public static final String TABLE_NAME_CONF_NAME = PREFIX + ".table.name";

  /** default value for domain table name. */
  public static final String DEFAULT_TABLE_NAME = "timelineservice.tic_to_app";

  public TicToAppTableRW() {
    super(TABLE_NAME_CONF_NAME, DEFAULT_TABLE_NAME);
  }

  @Override
  public void createTable(Admin admin, Configuration hbaseConf)
      throws IOException {

  }
}
