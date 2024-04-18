package org.apache.hadoop.yarn.logaggregation.filecontroller.hbase;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertTrue;

public class TestAggregatedLogHBaseFormat {

  @Test
  public void testContainerLogInputStream() throws IOException {
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    ContainerId containerId1 = ContainerId.newContainerId(appAttemptId, 1);

    Map<Long, byte[]> chunkMap = new HashMap<>();
    StringBuilder chunk0 = new StringBuilder();
    for (int i = 0; i < 8 * 1024; i++) {
      chunk0.append("a");
    }
    chunkMap.put(0l, chunk0.toString().getBytes());
    chunkMap.put(1l, "123456789abcdefghijklmnopqrstuvwxyz".getBytes());

    AggregatedLogHBaseFormat.LogReader chunkReader =
        new AggregatedLogHBaseFormat.LogReader(appId, containerId1, null) {

          public byte[] readContainerChunk(String fileName, long chunkId)
              throws IOException {
            return chunkMap.get(chunkId);
          }
        };

    AggregatedLogHBaseFormat.ContainerLogInputStream logReader =
        new AggregatedLogHBaseFormat.ContainerLogInputStream("a", 8227,
            chunkReader);

    byte[] buf = new byte[10];
    logReader.read(buf);
    Assert.assertEquals(new String(buf), "aaaaaaaaaa");
    logReader.skip(8182);
    logReader.read(buf);
    Assert.assertEquals(new String(buf), "123456789a");
    logReader.skip(80);
    Assert.assertEquals(-1, logReader.read());

    logReader.reset();
    logReader.skip(8191);
    Assert.assertEquals('a', (char) logReader.read());
    Assert.assertEquals('1', (char) logReader.read());

    byte[] buf1 = new byte[10000];
    logReader.reset();
    int readBytes = logReader.read(buf1);
    Assert.assertEquals(8227, readBytes);
  }
}
