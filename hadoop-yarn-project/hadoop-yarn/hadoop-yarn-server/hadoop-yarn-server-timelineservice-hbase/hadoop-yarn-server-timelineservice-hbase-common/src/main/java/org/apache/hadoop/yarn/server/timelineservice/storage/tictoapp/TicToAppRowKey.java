package org.apache.hadoop.yarn.server.timelineservice.storage.tictoapp;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverter;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class TicToAppRowKey {

  private final String tic;
  private final TicRowKeyConverter ticRowKeyConverter =
      new TicRowKeyConverter();

  public TicToAppRowKey(String tic) {
    this.tic = tic;
  }

  public String getTic() {
    return tic;
  }

  public byte[] getRowKey() {
    return ticRowKeyConverter.encode(this);
  }

  public static TicToAppRowKey parseRowKey(byte[] rowKey) {
    return new TicRowKeyConverter().decode(rowKey);
  }

  final private static class TicRowKeyConverter implements KeyConverter<TicToAppRowKey> {

    @Override
    public byte[] encode(TicToAppRowKey key) {
      String saltedRowKey;
      try {
        MessageDigest digest = MessageDigest.getInstance("MD5");
        saltedRowKey = saltRowKey(key.getTic(), digest);
      } catch (NoSuchAlgorithmException e) {
        e.printStackTrace();
        return null;
      }
      return Bytes.toBytes(saltedRowKey);
    }

    private static String saltRowKey(String rowKey, MessageDigest message) {
      byte[] digest = message.digest(Bytes.toBytes(rowKey));
      String md5 = new BigInteger(1, digest).toString(16).toLowerCase();
      StringBuilder builder = new StringBuilder();
      builder.append(md5, 0, 2)
          .append(":")
          .append(rowKey);
      return builder.toString();
    }

    @Override
    public TicToAppRowKey decode(byte[] bytes) {
      return new TicToAppRowKey(Bytes.toString(bytes).substring(3));
    }
  }
}
