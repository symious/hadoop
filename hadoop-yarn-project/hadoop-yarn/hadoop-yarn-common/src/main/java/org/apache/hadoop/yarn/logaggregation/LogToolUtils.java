/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.logaggregation;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;

/**
 * This class contains several utility function which could be used in different
 * log tools.
 *
 */
public final class LogToolUtils {

  private LogToolUtils() {}

  public static final String CONTAINER_ON_NODE_PATTERN =
      "Container: %s on %s";

  private static final Logger LOG =
      LoggerFactory.getLogger(LogToolUtils.class);

  /**
   * Formats the header of an aggregated log file.
   */
  private static byte[] formatContainerLogHeader(String containerId,
      String nodeId, ContainerLogAggregationType logType, String fileName,
      String lastModifiedTime, long fileLength) {
    StringBuilder sb = new StringBuilder();
    String containerStr = String.format(
        LogToolUtils.CONTAINER_ON_NODE_PATTERN,
        containerId, nodeId);
    sb.append(containerStr + "\n")
        .append("LogAggregationType: " + logType + "\n")
        .append(StringUtils.repeat("=", containerStr.length()) + "\n")
        .append("LogType:" + fileName + "\n")
        .append("LogLastModifiedTime:" + lastModifiedTime + "\n")
        .append("LogLength:" + fileLength + "\n")
        .append("LogContents:\n");
    return sb.toString().getBytes(Charset.forName("UTF-8"));
  }

  /**
   * Output container log.
   * @param containerId the containerId
   * @param nodeId the nodeId
   * @param fileName the log file name
   * @param fileLength the log file length
   * @param outputSize the output size
   * @param lastModifiedTime the log file last modified time
   * @param fis the log file input stream
   * @param os the output stream
   * @param buf the buffer
   * @param logType the log type
   * @param startIndex the start index of log to read.
   * @throws IOException if we can not access the log file.
   */
  public static void outputContainerLog(String containerId, String nodeId,
      String fileName, long fileLength, long outputSize,
      String lastModifiedTime, InputStream fis, OutputStream os,
      byte[] buf, ContainerLogAggregationType logType, long startIndex, boolean needLogMetaInfo) throws IOException {

    long toSkip = 0;
    long totalBytesToRead = fileLength;
    long skipAfterRead = 0;
    if (outputSize < 0) {
      long absBytes = Math.abs(outputSize);
      if (absBytes < fileLength) {
        toSkip = fileLength - absBytes;
        totalBytesToRead = absBytes;
      }
      org.apache.hadoop.io.IOUtils.skipFully(fis, toSkip);
    } else {
      if (startIndex > 0) {
        skipToStartIndex(startIndex, fis);
      }
      if (startIndex + outputSize >= fileLength) {
        totalBytesToRead = fileLength - startIndex;
      } else {
        totalBytesToRead = outputSize;
        skipAfterRead = fileLength - outputSize - startIndex;
      }
      LOG.debug("outputSize: " + outputSize + " totalBytesToRead: "
          + totalBytesToRead + " skipAfterRead: " + skipAfterRead);
    }

    long curRead = 0;
    long pendingRead = totalBytesToRead - curRead;
    int toRead = pendingRead > buf.length ? buf.length
        : (int) pendingRead;
    int len = fis.read(buf, 0, toRead);
    boolean keepGoing = (len != -1 && curRead < totalBytesToRead);

    if (needLogMetaInfo) {
      byte[] b = formatContainerLogHeader(containerId, nodeId, logType, fileName,
          lastModifiedTime, fileLength);
      os.write(b, 0, b.length);
    }
    while (keepGoing) {
      os.write(buf, 0, len);
      curRead += len;

      pendingRead = totalBytesToRead - curRead;
      toRead = pendingRead > buf.length ? buf.length
          : (int) pendingRead;
      len = fis.read(buf, 0, toRead);
      keepGoing = (len != -1 && curRead < totalBytesToRead);
    }
    org.apache.hadoop.io.IOUtils.skipFully(fis, skipAfterRead);
    os.flush();
  }

  /**
   * Skip to start index of log.
   * @param startIndex start index of log
   * @param fis the log file input stream.
   * @throws IOException if we can not access the log file.
   */
  private static void skipToStartIndex(long startIndex, InputStream fis)
      throws IOException {

    long totalSkipped = 0;
    while (totalSkipped < startIndex) {
      long ret = fis.skip(startIndex - totalSkipped);
      if (ret == 0) {
        //Read one byte
        int nextByte = fis.read();
        // Check if we have reached EOF
        if (nextByte == -1) {
          throw new IOException("Premature EOF from container log");
        }
        ret = 1;
      }
      totalSkipped += ret;
    }
  }

  /**
   *
   * Output container log.
   * @param containerId the containerId
   * @param nodeId the nodeId
   * @param fileName the log file name
   * @param fileLength the log file length
   * @param outputSize the output size
   * @param lastModifiedTime the log file last modified time
   * @param fis the log file input stream
   * @param os the output stream
   * @param buf the buffer
   * @param logType the log type.
   * @throws IOException if we can not access the log file.
   */
  public static void outputContainerLog(String containerId, String nodeId,
      String fileName, long fileLength, long outputSize,
      String lastModifiedTime, InputStream fis, OutputStream os,
      byte[] buf, ContainerLogAggregationType logType) throws IOException {

    outputContainerLog(containerId, nodeId, fileName, fileLength, outputSize,
        lastModifiedTime, fis, os, buf, logType, 0, true);
  }

  public static void outputContainerLogThroughZeroCopy(String containerId,
      String nodeId, String fileName, long fileLength, long outputSize,
      String lastModifiedTime, FileInputStream fis, OutputStream os,
      ContainerLogAggregationType logType, long startIndex) throws IOException {
    long toSkip = 0;
    long totalBytesToRead = fileLength;
    if (outputSize < 0) {
      long absBytes = Math.abs(outputSize);
      if (absBytes < fileLength) {
        toSkip = fileLength - absBytes;
        totalBytesToRead = absBytes;
      }
    } else {
      if (startIndex > 0) {
        toSkip = startIndex;
      } else {
        startIndex = 0;
      }
      if (outputSize + startIndex < outputSize) {
        outputSize = outputSize - startIndex;
      }
      if (startIndex + outputSize >= fileLength) {
        totalBytesToRead = fileLength - startIndex;
      } else {
        totalBytesToRead = outputSize;
      }
    }
    LOG.debug("toSkip: " + toSkip + " outputSize: " + outputSize
        + " totalBytesToRead: " + totalBytesToRead);
    if (totalBytesToRead > 0) {
      // output log content
      FileChannel inputChannel = fis.getChannel();
      WritableByteChannel outputChannel = Channels.newChannel(os);
      long position = toSkip;
      while (totalBytesToRead > 0) {
        long transferred =
            inputChannel.transferTo(position, totalBytesToRead, outputChannel);
        totalBytesToRead -= transferred;
        position += transferred;
      }
      os.flush();
    }
  }


  /**
   * Create the container log file under given (local directory/nodeId) and
   * return the PrintStream object.
   * @param localDir the Local Dir
   * @param nodeId the NodeId
   * @param containerId the ContainerId
   * @return the printStream object
   * @throws IOException if an I/O error occurs
   */
  public static PrintStream createPrintStream(String localDir, String nodeId,
      String containerId) throws IOException {
    PrintStream out = System.out;
    if(localDir != null && !localDir.isEmpty()) {
      Path nodePath = new Path(localDir, LogAggregationUtils
          .getNodeString(nodeId));
      Files.createDirectories(Paths.get(nodePath.toString()));
      Path containerLogPath = new Path(nodePath, containerId);
      out = new PrintStream(containerLogPath.toString(), "UTF-8");
    }
    return out;
  }

  /**
   * Redirect the {@link ContainerLogsRequest} to the NodeManager's
   * NMWebServices.
   *
   * @param conf Configuration object
   * @param webServiceClient client
   * @param request the request for container logs
   * @param logFile name of the log file
   * @return response from NMWebServices
   */
  public static ClientResponse getResponseFromNMWebService(Configuration conf,
      Client webServiceClient, ContainerLogsRequest request, String logFile) {
    WebResource webResource =
        webServiceClient.resource(WebAppUtils.getHttpSchemePrefix(conf)
            + request.getNodeHttpAddress());
    return webResource.path("ws").path("v1").path("node")
        .path("containers").path(request.getContainerId()).path("logs")
        .path(logFile)
        .queryParam("size", Long.toString(request.getBytes()))
        .accept(MediaType.TEXT_PLAIN).get(ClientResponse.class);
  }
}
