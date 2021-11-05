package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrCodec;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestXAttrStorage {
  private static final long BLOCK_ID = 1234;
  private static final short REPLICATION = 1;
  private static final long BLOCK_SIZE = 1024 * 1024;
  private static final int SNAPSHOT_ID = Snapshot.CURRENT_STATE_ID;
  private final PermissionStatus perm = new PermissionStatus(
      "hdfs", "supergroup", FsPermission.createImmutable((short) 0x1ff));

  @Test
  public void testUpdateINodeXAttrs() throws IOException {
    // get INode instance
    INodeFile file = new INodeFile(BLOCK_ID, null, perm,
        0L, 0L, null,REPLICATION, BLOCK_SIZE, (byte)0);
    Assert.assertNull(file.getXAttrFeature(SNAPSHOT_ID));

    String attrName1 = "user.replicationRule";
    String attrValue1 = "/Telin-3:2,/AirTrunk:1";
    byte[] value = XAttrCodec.decodeValue(attrValue1);
    XAttr xAttr = XAttrHelper.buildXAttr(attrName1, value);
    List<XAttr> xAttrs = new ArrayList<>();
    xAttrs.add(xAttr);
    XAttrStorage.updateINodeXAttrs(file, xAttrs, SNAPSHOT_ID);
    XAttrFeature feature1 = file.getXAttrFeature(SNAPSHOT_ID);
    Assert.assertNotNull(feature1);

    String attrName2 = "user.replicationRule2";
    XAttr xAttr2 = XAttrHelper.buildXAttr(attrName2, value);
    List<XAttr> xAttrs2 = new ArrayList<>();
    xAttrs2.add(xAttr2);
    XAttrStorage.updateINodeXAttrs(file, xAttrs2, SNAPSHOT_ID);
    XAttrFeature feature2 = file.getXAttrFeature(SNAPSHOT_ID);
    Assert.assertNotSame(feature1, feature2);

    XAttrStorage.updateINodeXAttrs(file, xAttrs, SNAPSHOT_ID);
    XAttrFeature feature3 = file.getXAttrFeature(SNAPSHOT_ID);
    Assert.assertSame(feature1, feature3);
  }

  @Test
  public void testCreateXAttrFeature() throws IOException {
    String attrName1 = "user.replicationRule";
    String attrValue1 = "/Telin-3:2,/AirTrunk:1";
    byte[] value = XAttrCodec.decodeValue(attrValue1);
    XAttr xAttr = XAttrHelper.buildXAttr(attrName1, value);
    List<XAttr> xAttrs = new ArrayList<>();
    xAttrs.add(xAttr);
    XAttrFeature feature1 = XAttrStorage.createXAttrFeature(xAttrs);
    Assert.assertNotNull(feature1);

    String attrName2 = "user.replicationRule2";
    XAttr xAttr2 = XAttrHelper.buildXAttr(attrName2, value);
    List<XAttr> xAttrs2 = new ArrayList<>();
    xAttrs2.add(xAttr2);
    XAttrFeature feature2 = XAttrStorage.createXAttrFeature(xAttrs2);
    Assert.assertNotSame(feature1, feature2);

    XAttrFeature feature3 = XAttrStorage.createXAttrFeature(xAttrs);
    Assert.assertSame(feature1, feature3);
  }
}
