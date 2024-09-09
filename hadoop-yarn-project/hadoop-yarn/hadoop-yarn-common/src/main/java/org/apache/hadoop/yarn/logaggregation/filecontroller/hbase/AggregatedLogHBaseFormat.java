package org.apache.hadoop.yarn.logaggregation.filecontroller.hbase;

import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogKey;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogValue;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import java.util.stream.Collectors;

public class AggregatedLogHBaseFormat {

  private final static Logger LOG =
      LoggerFactory.getLogger(AggregatedLogHBaseFormat.class);

  public static final String TABLE_NAME_CONF_NAME = "yarn.log.table.name";

  public static final String DEFAULT_TABLE_NAME = "log";

  private final static String SEPARATOR = "!";
  private final static String FILE_LIST = "file_list";

  private final static int CHUNK_SIZE = 8 * 1024;

  public static class LogReader {
    private byte[] rowKey;
    private LogAggregationHBaseController.HBaseReader hbaseReader;

    public LogReader(ApplicationId appId, ContainerId containerId,
        LogAggregationHBaseController.HBaseReader hbaseReader)
        throws IOException {
      this.rowKey = getRowKey(appId, containerId);
      this.hbaseReader = hbaseReader;
    }

    public Pair<String, Long> readContainerFileMetaData(String fileName)
        throws IOException {
      byte[] value = hbaseReader.get(rowKey, LogColumnFamily.META.getBytes(),
          Bytes.toBytes(fileName));
      if (value == null) {
        throw new IOException(
            "Container file: " + fileName + " meta is not exist.");
      }
      long length = Bytes.toLong(value);
      return new Pair<String, Long>(fileName, length);
    }

    public List<String> readContainerFileList() throws IOException {
      byte[] value = hbaseReader.get(rowKey, LogColumnFamily.META.getBytes(),
          Bytes.toBytes(FILE_LIST));
      if (value == null) {
        throw new IOException("Container file list is not exist.");
      }
      return Arrays.asList(Bytes.toString(value).split(SEPARATOR));
    }

    public byte[] readContainerChunk(String fileName, long chunkId)
        throws IOException {
      byte[] value = hbaseReader.get(rowKey, LogColumnFamily.LOG.getBytes(),
          Bytes.toBytes(fileName + SEPARATOR + chunkId));
      if (value == null) {
        throw new IOException("FileName: " + fileName + " chunkId: " + chunkId
            + " is not exist.");
      }
      return value;
    }

    public InputStream getFileInputStream(String file, long length) {
      return new ContainerLogInputStream(file, length, this);
    }
  }

  public static class ContainerLogInputStream extends InputStream {

    private String fileName;
    private long fileLength;
    private long currChunkId = -1;
    private byte[] currChunk = null;
    private int pos = 0;
    private int remaining = 0;
    private long offset = 0;

    private LogReader chunkReader;

    public ContainerLogInputStream(String fileName, long fileLength,
        LogReader chunkReader) {
      this.fileName = fileName;
      this.fileLength = fileLength;
      this.chunkReader = chunkReader;
    }

    @Override
    public int read() throws IOException {
      if (offset >= fileLength) {
        return -1;
      }
      if (remaining <= 0) {
        readChunk();
      }
      int result = currChunk[pos];
      pos++;
      offset++;
      remaining--;
      return result;
    }

    public int read(byte[] b) throws IOException {
      return this.read(b, 0, b.length);
    }

    public int read(byte[] b, int off, int len) throws IOException {
      if (b == null) {
        throw new NullPointerException();
      } else if (off >= 0 && len >= 0 && len <= b.length - off) {
        long pendingRead =
            (offset + len > fileLength) ? fileLength - offset : len;
        if (pendingRead == 0)
          return -1;
        int alreadyRead = 0;
        while (pendingRead > 0) {
          if (remaining <= 0) {
            readChunk();
          }
          if (pendingRead >= remaining) {
            System.arraycopy(currChunk, pos, b, off + alreadyRead, remaining);
            pendingRead -= remaining;
            alreadyRead += remaining;
            pos += remaining;
            offset += remaining;
            remaining = 0;
          } else {
            System.arraycopy(currChunk, pos, b, off + alreadyRead,
                (int) pendingRead);
            alreadyRead += pendingRead;
            pos += pendingRead;
            offset += pendingRead;
            remaining -= pendingRead;
            pendingRead = 0;
          }
        }
        return alreadyRead;
      } else {
        throw new IndexOutOfBoundsException();
      }
    }

    public long skip(long n) throws IOException {
      long globalCurrPos = offset;
      long globalTatPos =
          (globalCurrPos + n) >= fileLength ? fileLength : (globalCurrPos + n);
      long skip = globalTatPos - globalCurrPos;
      offset = globalTatPos;
      if (globalTatPos == fileLength)
        return skip;

      long targetChunkId = (globalTatPos - 1) / CHUNK_SIZE;
      if (targetChunkId != currChunkId) {
        currChunkId = targetChunkId - 1;
        readChunk();
        pos = (int) (globalTatPos - targetChunkId * CHUNK_SIZE);
      } else {
        pos += globalTatPos - globalCurrPos;
      }
      remaining = currChunk.length - pos;
      return skip;
    }

    private void readChunk() throws IOException {
      currChunk = chunkReader.readContainerChunk(fileName, ++currChunkId);
      pos = 0;
      remaining = currChunk.length;
    }

    public void reset() {
      currChunk = null;
      currChunkId = -1;
      pos = 0;
      offset = 0;
      remaining = 0;
    }

  }

  public static class LogWriter implements AutoCloseable {

    private ApplicationId appId;
    private ContainerId containerId;
    private LogAggregationHBaseController.HBaseWriter hbaseWriter;

    public void initialize(ApplicationId appId,
        LogAggregationHBaseController.HBaseWriter writer) throws IOException {
      this.appId = appId;
      this.hbaseWriter = writer;
    }

    private class ChunkEncoder extends OutputStream {
      private LogWriter writer;
      private byte[] buf;
      private String fileName;
      private int count;
      private int chunkId;

      public ChunkEncoder(LogWriter writer, int chunkSize, String fileName) {
        this.writer = writer;
        this.buf = new byte[chunkSize];
        this.fileName = fileName;
        this.count = 0;
        this.chunkId = 0;
      }

      @Override
      public void write(int b) throws IOException {
      }

      @Override
      public void write(byte[] b, int off, int len) throws IOException {
        int leftBytes = len;
        int offset = off;
        while (leftBytes > 0) {
          if (this.count + leftBytes >= this.buf.length) {
            int remain = this.buf.length - this.count;
            System.arraycopy(b, offset, this.buf, this.count, remain);
            offset += remain;
            leftBytes -= remain;
            this.count += remain;
          } else {
            System.arraycopy(b, offset, this.buf, this.count, leftBytes);
            this.count += leftBytes;
            leftBytes = 0;
          }
          flushBuffer();
        }
      }

      private void flushBuffer() throws IOException {
        if (this.count >= this.buf.length) {
          writer.writeLog(this.buf, fileName, chunkId++);
          this.count = 0;
        }
      }

      public void flush() throws IOException {
        if (this.buf != null && count != 0) {
          writer
              .writeLog(Arrays.copyOfRange(buf, 0, count), fileName, chunkId++);
        }
      }
    }

    public void append(LogKey logKey, LogValue logValue) {
      resetContainerId(ContainerId.fromString(logKey.toString()));
      Set<File> pendingUploadFiles =
          logValue.getPendingLogFilesToUploadForThisContainer();
      if (pendingUploadFiles.size() == 0) {
        return;
      }
      List<File> fileList = new ArrayList<File>(pendingUploadFiles);
      List<Pair<String, Long>> logMetaList = new ArrayList<>();
      Collections.sort(fileList);
      for (File logFile : fileList) {
        if (logFile.isDirectory()) {
          LOG.warn(logFile.getAbsolutePath() + " is a directory. Ignore it.");
          continue;
        }
        FileInputStream in = null;
        ChunkEncoder out = null;
        try {
          in = new FileInputStream(logFile);
          out = new ChunkEncoder(this, CHUNK_SIZE, logFile.getName());
        } catch (IOException e) {
          continue;
        }
        logMetaList.add(new Pair<>(logFile.getName(), logFile.length()));
        try {
          byte[] buf = new byte[65535];
          int len = 0;
          while ((len = in.read(buf)) != -1) {
            out.write(buf, 0, len);
          }
          out.flush();
        } catch (IOException e) {
        } finally {
          IOUtils.cleanupWithLogger(LOG, in, out);
        }
      }
      try {
        writeMetaData(logMetaList);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    public void resetContainerId(ContainerId containerId) {
      this.containerId = containerId;
    }

    public void writeLog(byte[] logContent, String fileName, int chunkId)
        throws IOException {
      hbaseWriter
          .write(getRowKey(appId, containerId), LogColumnFamily.LOG.getBytes(),
              Bytes.toBytes(fileName + SEPARATOR + chunkId), logContent);
    }

    public void writeMetaData(List<Pair<String, Long>> logMetaList)
        throws IOException {
      for (Pair<String, Long> logMeta : logMetaList) {
        String file = logMeta.getFirst();
        long length = logMeta.getSecond();
        hbaseWriter.write(getRowKey(appId, containerId),
            LogColumnFamily.META.getBytes(), Bytes.toBytes(file),
            Bytes.toBytes(length));
      }
      Optional<String> fileList = logMetaList.stream().map(o -> o.getFirst())
          .collect(Collectors.reducing((o1, o2) -> o1 + SEPARATOR + o2));
      if (fileList.isPresent()) {
        hbaseWriter.write(getRowKey(appId, containerId),
            LogColumnFamily.META.getBytes(), Bytes.toBytes(FILE_LIST),
            fileList.get().getBytes());
      }
    }

    @Override
    public void close() {
    }
  }

  public static byte[] getRowKey(ApplicationId appId, ContainerId containerId) {
    int id = appId.getId();
    int suffix = id % 100000;
    String strSuffix = String.format("%05d", suffix);
    StringBuilder builder = new StringBuilder();
    byte[] first = builder.append(strSuffix).append(":").toString().getBytes();
    byte[] second = encodeAppId(appId);
    byte[] third = encodeContainerId(containerId);
    return join(first, second, third);
  }

  public static byte[] encodeAppId(ApplicationId appId) {
    byte[] appIdBytes = new byte[Bytes.SIZEOF_LONG + Bytes.SIZEOF_INT];
    byte[] clusterTs = Bytes.toBytes(appId.getClusterTimestamp());
    System.arraycopy(clusterTs, 0, appIdBytes, 0, Bytes.SIZEOF_LONG);
    byte[] seqId = Bytes.toBytes(appId.getId());
    System.arraycopy(seqId, 0, appIdBytes, Bytes.SIZEOF_LONG, Bytes.SIZEOF_INT);
    return appIdBytes;
  }

  public static byte[] encodeContainerId(ContainerId containerId) {
    byte[] containerIdBytes = new byte[Bytes.SIZEOF_INT + Bytes.SIZEOF_LONG];
    byte[] attemptId =
        Bytes.toBytes(containerId.getApplicationAttemptId().getAttemptId());
    System.arraycopy(attemptId, 0, containerIdBytes, 0, Bytes.SIZEOF_INT);
    byte[] seqId = Bytes.toBytes(containerId.getContainerId());
    System.arraycopy(seqId, 0, containerIdBytes, Bytes.SIZEOF_INT,
        Bytes.SIZEOF_LONG);
    return containerIdBytes;
  }

  public static byte[] join(byte[]... components) {
    if (components == null || components.length == 0) {
      return new byte[0];
    }
    int finalSize = 0;
    finalSize = SEPARATOR.length() * (components.length - 1);
    for (byte[] comp : components) {
      if (comp != null) {
        finalSize += comp.length;
      }
    }
    byte[] buf = new byte[finalSize];
    int offset = 0;
    for (int i = 0; i < components.length; i++) {
      if (components[i] != null) {
        System.arraycopy(components[i], 0, buf, offset, components[i].length);
        offset += components[i].length;
      }
      if (i < (components.length - 1)) {
        System.arraycopy(SEPARATOR.getBytes(), 0, buf, offset,
            SEPARATOR.length());
        offset += SEPARATOR.length();
      }
    }
    return buf;
  }
}
