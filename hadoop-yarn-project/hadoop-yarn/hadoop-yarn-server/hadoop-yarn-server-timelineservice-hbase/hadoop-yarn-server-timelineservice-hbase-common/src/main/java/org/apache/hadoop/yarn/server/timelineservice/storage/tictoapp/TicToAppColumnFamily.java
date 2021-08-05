package org.apache.hadoop.yarn.server.timelineservice.storage.tictoapp;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnFamily;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;

public enum TicToAppColumnFamily implements ColumnFamily<TicToAppTable> {

  INFO("i");

  private final byte[] bytes;

  TicToAppColumnFamily(String value) {
    this.bytes = Bytes.toBytes(Separator.SPACE.encode(value));
  }

  @Override
  public byte[] getBytes() {
    return Bytes.copy(bytes);
  }
}
