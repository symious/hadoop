package org.apache.hadoop.yarn.server.timelineservice.storage.tictoapp;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.*;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.Attribute;

public enum TicToAppColumn implements Column<TicToAppTable> {
  ID(TicToAppColumnFamily.INFO, "id");

  private final ColumnFamily<TicToAppTable> columnFamily;
  private final String columnQualifier;
  private final byte[] columnQualifierBytes;
  private final ValueConverter valueConverter;

  TicToAppColumn(ColumnFamily<TicToAppTable> columnFamily,
      String columnQualifier) {
    this.columnFamily = columnFamily;
    this.columnQualifier = columnQualifier;
    this.columnQualifierBytes = Bytes.toBytes(Separator.SPACE.encode(columnQualifier));
    this.valueConverter = GenericConverter.getInstance();
  }

  @Override
  public byte[] getColumnFamilyBytes() {
    return columnFamily.getBytes();
  }

  @Override
  public byte[] getColumnQualifierBytes() {
    return columnQualifierBytes.clone();
  }

  @Override
  public ValueConverter getValueConverter() {
    return valueConverter;
  }

  @Override
  public Attribute[] getCombinedAttrsWithAggr(Attribute... attributes) {
    return attributes;
  }

  @Override
  public boolean supplementCellTimestamp() {
    return false;
  }
}
