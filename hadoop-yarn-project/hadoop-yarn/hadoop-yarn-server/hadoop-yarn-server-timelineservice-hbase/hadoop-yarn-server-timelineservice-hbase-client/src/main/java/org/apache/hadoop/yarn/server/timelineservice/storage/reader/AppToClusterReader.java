package org.apache.hadoop.yarn.server.timelineservice.storage.reader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderContext;
import org.apache.hadoop.yarn.server.timelineservice.storage.apptoflow.AppToFlowRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.apptoflow.AppToFlowTableRW;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class AppToClusterReader extends AbstractTimelineStorageReader {

  private final AppToFlowTableRW appToFlowTable = new AppToFlowTableRW();

  public AppToClusterReader(TimelineReaderContext ctxt) {
    super(ctxt);
  }

  @Override
  protected void validateParams() {
  }

  public Set<String> readEntityTypes(Configuration hbaseConf, Connection conn)
      throws IOException {
    Set<String> types = new TreeSet<>();
    TimelineReaderContext context = getContext();
    AppToFlowRowKey appKey = new AppToFlowRowKey(context.getAppId());
    byte[] rowKey = appKey.getRowKey();
    Get get = new Get(rowKey);
    Filter familyFilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL,
        new BinaryComparator(Bytes.toBytes("m")));
    Filter columnFilter = new ColumnPrefixFilter(Bytes.toBytes("flow_name"));
    FilterList filterList = new FilterList();
    filterList.addFilter(familyFilter);
    filterList.addFilter(columnFilter);
    get.setFilter(filterList);
    Result result = appToFlowTable.getResult(hbaseConf, conn, get);
    Map<byte[], byte[]> familyMap = result.getFamilyMap(Bytes.toBytes("m"));
    for (Map.Entry<byte[], byte[]> entry : familyMap.entrySet()) {
      types.add(Bytes.toString(entry.getKey()).split("!")[1]);
    }
    return types;
  }
}
