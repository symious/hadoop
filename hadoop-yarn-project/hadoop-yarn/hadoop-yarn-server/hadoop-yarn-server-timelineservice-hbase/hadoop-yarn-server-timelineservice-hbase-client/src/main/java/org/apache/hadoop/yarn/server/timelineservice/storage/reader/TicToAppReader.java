package org.apache.hadoop.yarn.server.timelineservice.storage.reader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineDataToRetrieve;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderContext;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.BaseTableRW;
import org.apache.hadoop.yarn.server.timelineservice.storage.tictoapp.TicToAppColumn;
import org.apache.hadoop.yarn.server.timelineservice.storage.tictoapp.TicToAppRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.tictoapp.TicToAppTableRW;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class TicToAppReader extends TimelineEntityReader {
  private static final TicToAppTableRW TIC_TO_APP_TABLE = new TicToAppTableRW();

  TicToAppReader(TimelineReaderContext ctxt,
      TimelineDataToRetrieve toRetrieve) {
    super(ctxt, toRetrieve);
  }

  @Override
  protected BaseTableRW<?> getTable() {
    return TIC_TO_APP_TABLE;
  }

  @Override
  protected void validateParams() {
  }

  @Override
  protected void augmentParams(Configuration hbaseConf, Connection conn) {
  }

  @Override
  protected FilterList constructFilterListBasedOnFields(Set<String> cfsInFields)
      throws IOException {
    return null;
  }

  @Override
  protected FilterList constructFilterListBasedOnFilters() throws IOException {
    return null;
  }

  @Override
  protected Result getResult(Configuration hbaseConf, Connection conn,
      FilterList filterList) throws IOException {
    TicToAppRowKey rowKey = new TicToAppRowKey(getContext().getEntityId());
    Get get = new Get(rowKey.getRowKey());
    return getTable().getResult(hbaseConf, conn, get);
  }

  @Override
  protected ResultScanner getResults(Configuration hbaseConf, Connection conn,
      FilterList filterList) throws IOException {
    return null;
  }

  @Override
  protected TimelineEntity parseEntity(Result result) throws IOException {
    List<Cell> list = result
        .getColumnCells(TicToAppColumn.ID.getColumnFamilyBytes(),
            TicToAppColumn.ID.getColumnQualifierBytes());
    TimelineEntity entity = new TimelineEntity();
    entity.setId(Bytes.toString(list.get(0).getValueArray()));
    return entity;
  }
}
