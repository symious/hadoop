package org.apache.hadoop.yarn.logaggregation.filecontroller.hbase;

import org.apache.hadoop.hbase.util.Bytes;

public enum LogColumnFamily {

  META("m"), LOG("l");

  private final byte[] bytes;

  LogColumnFamily(String value) {
    this.bytes = Bytes.toBytes(value);
  }

  public byte[] getBytes() {
    return Bytes.copy(bytes);
  }
}
