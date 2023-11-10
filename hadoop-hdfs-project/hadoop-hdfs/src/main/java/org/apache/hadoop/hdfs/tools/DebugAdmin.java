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
package org.apache.hadoop.hdfs.tools;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.util.ECBlockValidatorReport;
import org.apache.hadoop.hdfs.util.ECFileValidator;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.Uninterruptibles;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetUtil;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements debug operations on the HDFS command-line.
 *
 * These operations are only for debugging, and may change or disappear
 * between HDFS versions.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DebugAdmin extends Configured implements Tool {
  private static final Logger LOG = LoggerFactory.getLogger(DebugAdmin.class);

  /**
   * All the debug commands we can run.
   */
  private final DebugCommand[] DEBUG_COMMANDS = {
      new VerifyMetaCommand(),
      new ComputeMetaCommand(),
      new RecoverLeaseCommand(),
      new VerifyECCommand(),
      new ReadWithDNPreference(),
      new WriteWithDNPreference(),
      new HelpCommand()
  };

  /**
   * The base class for debug commands.
   */
  private abstract static class DebugCommand {
    final String name;
    final String usageText;
    final String helpText;

    DebugCommand(String name, String usageText, String helpText) {
      this.name = name;
      this.usageText = usageText;
      this.helpText = helpText;
    }

    abstract int run(List<String> args) throws IOException;
  }

  private static int HEADER_LEN = 7;

  /**
   * The command for verifying a block metadata file and possibly block file.
   */
  private static class VerifyMetaCommand extends DebugCommand {
    VerifyMetaCommand() {
      super("verifyMeta",
          "verifyMeta -meta <metadata-file> [-block <block-file>]",
          "  Verify HDFS metadata and block files.  If a block file is specified, we" +
              System.lineSeparator() +
              "  will verify that the checksums in the metadata file match the block" +
              System.lineSeparator() +
              "  file.");
    }

    int run(List<String> args) throws IOException {
      if (args.size() == 0) {
        System.out.println(usageText);
        System.out.println(helpText + System.lineSeparator());
        return 1;
      }
      String blockFile = StringUtils.popOptionWithArgument("-block", args);
      String metaFile = StringUtils.popOptionWithArgument("-meta", args);
      if (metaFile == null) {
        System.err.println("You must specify a meta file with -meta");
        return 1;
      }

      FileInputStream metaStream = null, dataStream = null;
      FileChannel metaChannel = null, dataChannel = null;
      DataInputStream checksumStream = null;
      try {
        BlockMetadataHeader header;
        try {
          metaStream = new FileInputStream(metaFile);
          checksumStream = new DataInputStream(metaStream);
          header = BlockMetadataHeader.readHeader(checksumStream);
          metaChannel = metaStream.getChannel();
          metaChannel.position(HEADER_LEN);
        } catch (RuntimeException e) {
          System.err.println("Failed to read HDFS metadata file header for " +
              metaFile + ": " + StringUtils.stringifyException(e));
          return 1;
        } catch (IOException e) {
          System.err.println("Failed to read HDFS metadata file header for " +
              metaFile + ": " + StringUtils.stringifyException(e));
          return 1;
        }
        DataChecksum checksum = header.getChecksum();
        System.out.println("Checksum type: " + checksum.toString());
        if (blockFile == null) {
          return 0;
        }
        ByteBuffer metaBuf, dataBuf;
        try {
          dataStream = new FileInputStream(blockFile);
          dataChannel = dataStream.getChannel();
          final int CHECKSUMS_PER_BUF = 1024 * 32;
          metaBuf = ByteBuffer.allocate(checksum.
              getChecksumSize() * CHECKSUMS_PER_BUF);
          dataBuf = ByteBuffer.allocate(checksum.
              getBytesPerChecksum() * CHECKSUMS_PER_BUF);
        } catch (IOException e) {
          System.err.println("Failed to open HDFS block file for " +
              blockFile + ": " + StringUtils.stringifyException(e));
          return 1;
        }
        long offset = 0;
        while (true) {
          dataBuf.clear();
          int dataRead = -1;
          try {
            dataRead = dataChannel.read(dataBuf);
            if (dataRead < 0) {
              break;
            }
          } catch (IOException e) {
            System.err.println("Got I/O error reading block file " +
                blockFile + "from disk at offset " + dataChannel.position() +
                ": " + StringUtils.stringifyException(e));
            return 1;
          }
          try {
            int csumToRead =
                (((checksum.getBytesPerChecksum() - 1) + dataRead) /
                  checksum.getBytesPerChecksum()) *
                      checksum.getChecksumSize();
            metaBuf.clear();
            metaBuf.limit(csumToRead);
            metaChannel.read(metaBuf);
            dataBuf.flip();
            metaBuf.flip();
          } catch (IOException e) {
            System.err.println("Got I/O error reading metadata file " +
                metaFile + "from disk at offset " + metaChannel.position() +
                ": " +  StringUtils.stringifyException(e));
            return 1;
          }
          try {
            checksum.verifyChunkedSums(dataBuf, metaBuf,
                blockFile, offset);
          } catch (IOException e) {
            System.out.println("verifyChunkedSums error: " +
                StringUtils.stringifyException(e));
            return 1;
          }
          offset += dataRead;
        }
        System.out.println("Checksum verification succeeded on block file " +
            blockFile);
        return 0;
      } finally {
        IOUtils.cleanupWithLogger(null, metaStream, dataStream, checksumStream);
      }
    }
  }

  /**
   * The command for verifying a block metadata file and possibly block file.
   */
  private static class ComputeMetaCommand extends DebugCommand {
    ComputeMetaCommand() {
      super("computeMeta",
          "computeMeta -block <block-file> -out <output-metadata-file>",
          "  Compute HDFS metadata from the specified block file, and save it"
              + " to" + System.lineSeparator()
              + "  the specified output metadata file."
              + System.lineSeparator() + System.lineSeparator()
              + "**NOTE: Use at your own risk!" + System.lineSeparator()
              + " If the block file is corrupt"
              + " and you overwrite it's meta file, " + System.lineSeparator()
              + " it will show up"
              + " as good in HDFS, but you can't read the data."
              + System.lineSeparator()
              + " Only use as a last measure, and when you are 100% certain"
              + " the block file is good.");
    }

    private DataChecksum createChecksum(Options.ChecksumOpt opt) {
      DataChecksum dataChecksum = DataChecksum
          .newDataChecksum(opt.getChecksumType(), opt.getBytesPerChecksum());
      if (dataChecksum == null) {
        throw new HadoopIllegalArgumentException(
            "Invalid checksum type: userOpt=" + opt + ", default=" + opt
                + ", effective=null");
      }
      return dataChecksum;
    }

    int run(List<String> args) throws IOException {
      if (args.size() == 0) {
        System.out.println(usageText);
        System.out.println(helpText + System.lineSeparator());
        return 1;
      }
      final String name = StringUtils.popOptionWithArgument("-block", args);
      if (name == null) {
        System.err.println("You must specify a block file with -block");
        return 2;
      }
      final File blockFile = new File(name);
      if (!blockFile.exists() || !blockFile.isFile()) {
        System.err.println("Block file <" + name + "> does not exist "
            + "or is not a file");
        return 3;
      }
      final String outFile = StringUtils.popOptionWithArgument("-out", args);
      if (outFile == null) {
        System.err.println("You must specify a output file with -out");
        return 4;
      }
      final File srcMeta = new File(outFile);
      if (srcMeta.exists()) {
        System.err.println("output file already exists!");
        return 5;
      }

      DataOutputStream metaOut = null;
      try {
        final Configuration conf = new Configuration();
        final Options.ChecksumOpt checksumOpt =
            DfsClientConf.getChecksumOptFromConf(conf);
        final DataChecksum checksum = createChecksum(checksumOpt);

        final int smallBufferSize = DFSUtilClient.getSmallBufferSize(conf);
        metaOut = new DataOutputStream(
            new BufferedOutputStream(Files.newOutputStream(srcMeta.toPath()),
                smallBufferSize));
        BlockMetadataHeader.writeHeader(metaOut, checksum);
        metaOut.close();
        FsDatasetUtil.computeChecksum(
            srcMeta, srcMeta, blockFile, smallBufferSize, conf);
        System.out.println(
            "Checksum calculation succeeded on block file " + name
                + " saved metadata to meta file " + outFile);
        return 0;
      } finally {
        IOUtils.cleanupWithLogger(null, metaOut);
      }
    }
  }

  /**
   * The command for recovering a file lease.
   */
  private class RecoverLeaseCommand extends DebugCommand {
    RecoverLeaseCommand() {
      super("recoverLease",
"recoverLease -path <path> [-retries <num-retries>]",
"  Recover the lease on the specified path.  The path must reside on an" +
    System.lineSeparator() +
"  HDFS filesystem.  The default number of retries is 1.");
    }

    private static final int TIMEOUT_MS = 5000;

    int run(List<String> args) throws IOException {
      if (args.size() == 0) {
        System.out.println(usageText);
        System.out.println(helpText + System.lineSeparator());
        return 1;
      }
      String pathStr = StringUtils.popOptionWithArgument("-path", args);
      String retriesStr = StringUtils.popOptionWithArgument("-retries", args);
      if (pathStr == null) {
        System.err.println("You must supply a -path argument to " +
            "recoverLease.");
        return 1;
      }
      int maxRetries = 1;
      if (retriesStr != null) {
        try {
          maxRetries = Integer.parseInt(retriesStr);
        } catch (NumberFormatException e) {
          System.err.println("Failed to parse the argument to -retries: " +
              StringUtils.stringifyException(e));
          return 1;
        }
      }
      FileSystem fs;
      try {
        fs = FileSystem.newInstance(new URI(pathStr), getConf(), null);
      } catch (URISyntaxException e) {
        System.err.println("URISyntaxException for " + pathStr + ":" +
            StringUtils.stringifyException(e));
        return 1;
      } catch (InterruptedException e) {
        System.err.println("InterruptedException for " + pathStr + ":" +
            StringUtils.stringifyException(e));
        return 1;
      }
      DistributedFileSystem dfs = null;
      try {
        dfs = (DistributedFileSystem) fs;
      } catch (ClassCastException e) {
        System.err.println("Invalid filesystem for path " + pathStr + ": " +
            "needed scheme hdfs, but got: " + fs.getScheme());
        return 1;
      }
      for (int retry = 0; true; ) {
        boolean recovered = false;
        IOException ioe = null;
        try {
          recovered = dfs.recoverLease(new Path(pathStr));
        } catch (FileNotFoundException e) {
          System.err.println("recoverLease got exception: " + e.getMessage());
          System.err.println("Giving up on recoverLease for " + pathStr +
              " after 1 try");
          return 1;
        } catch (IOException e) {
          ioe = e;
        }
        if (recovered) {
          System.out.println("recoverLease SUCCEEDED on " + pathStr); 
          return 0;
        }
        if (ioe != null) {
          System.err.println("recoverLease got exception: " +
              ioe.getMessage());
        } else {
          System.err.println("recoverLease returned false.");
        }
        retry++;
        if (retry >= maxRetries) {
          break;
        }
        System.err.println("Retrying in " + TIMEOUT_MS + " ms...");
        Uninterruptibles.sleepUninterruptibly(TIMEOUT_MS,
            TimeUnit.MILLISECONDS);
        System.err.println("Retry #" + retry);
      }
      System.err.println("Giving up on recoverLease for " + pathStr + " after " +
          maxRetries + (maxRetries == 1 ? " try." : " tries."));
      return 1;
    }
  }

  /**
   * The command for verifying the correctness of erasure coding on an erasure coded file.
   */
  private class VerifyECCommand extends DebugCommand {
    private DFSClient client;

    VerifyECCommand() {
      super("verifyEC",
          "verifyEC -file <file>",
          "  Verify HDFS erasure coding on all block groups of the file.");
    }

    int run(List<String> args) throws IOException {
      if (args.size() < 2) {
        System.out.println(usageText);
        System.out.println(helpText + System.lineSeparator());
        return 1;
      }
      String file = StringUtils.popOptionWithArgument("-file", args);

      DistributedFileSystem dfs = AdminHelper.getDFS(getConf());

      boolean isHealthy = true;

      try (ECFileValidator ecFileValidator = new ECFileValidator(dfs)) {
        List<ECBlockValidatorReport> ecBlockValidatorReports = ecFileValidator.verifyECFile(file,
            true);
        for (ECBlockValidatorReport ecBlockValidatorReport : ecBlockValidatorReports) {
          if (ecBlockValidatorReport.getBlockGroup().equals(
              ecFileValidator.EC_FILE_FAIL_BLOCK)) {
            System.err.println(ecBlockValidatorReport.getMessage());
            return 1;
          }
          System.out.println("Checking EC block group: " + ecBlockValidatorReport.getBlockGroup());
          if (ecBlockValidatorReport.isHealthy()) {
            System.out.println("Status: OK");
          } else if (ecBlockValidatorReport.isUnder()) {
            isHealthy = false;
            System.err.println("Status: ERROR, message: Block group is under-erasure-coded.");
          } else if (ecBlockValidatorReport.isCorrupt()) {
            isHealthy = false;
            System.err.println("Status: ERROR, message: EC compute result not match.");
          } else {
            isHealthy = false;
            if (ecBlockValidatorReport.isCheckSumFailed()) {
              System.err.println("Status: ERROR, message: " +
                  ecBlockValidatorReport.checkSumFailedBlockReports().toString());
            }
            if (ecBlockValidatorReport.isFailed()) {
              System.err.println("Status: ERROR, message: " +
                  ecBlockValidatorReport.failedBlockReports().toString());
            }
          }
        }
        if (isHealthy) {
          System.out.println("All EC block group status: OK");
          return 0;
        }
      }
      return 1;
    }
  }

  /**
   * A tool to test DN connections by reading/writing a set of preferred and ignored nodes
   */
  private abstract class RWWithDNPreferenceBase extends DebugCommand {
    private Set<InetSocketAddress> favoredNodes = new HashSet<>();
    private Set<InetSocketAddress> ignoredNodes = new HashSet<>();
    protected DistributedFileSystem dfs;

    RWWithDNPreferenceBase(String name, String usageText, String helpText) {
      super(name, usageText, helpText);
    }

    int run(List<String> args) throws IOException {
      if (args.size() < 1) {
        System.out.println(usageText);
        System.out.println(helpText + System.lineSeparator());
        return 0;
      }

      handleArguments(args);
      String hdfsPath = args.remove(args.size() - 1);
      processPath(hdfsPath, args);
      return 0;
    }

    private void handleArguments(List<String> args) throws IOException {
      String includes = StringUtils.popOptionWithArgument("-favored", args);
      String excludes = StringUtils.popOptionWithArgument("-excluded", args);

      setDatanodePreference(includes, excludes);

      dfs = AdminHelper.getDFS(getConf());
      dfs.getClient()
          .setDatanodePreference(new DFSClient.DatanodePreference(favoredNodes, ignoredNodes));
    }

    /**
     * Main logic, need to implement this for child classes
     */
    abstract protected void processPath(String hdfsPath, List<String> leftoverArgs)
        throws IOException;

    private void setDatanodePreference(String favored, String ignored) throws IOException {
      addDNs(favored, favoredNodes);
      addDNs(ignored, ignoredNodes);
    }

    private void addDNs(String dnsString, Set<InetSocketAddress> nodes) {
      if (dnsString == null) {
        return;
      }
      String[] dns = dnsString.split(",");
      for (String ip : dns) {
        if (ip.startsWith("/")) {
          ip = ip.substring(1);
        }
        String[] split = ip.split(":");
        InetSocketAddress dn = new InetSocketAddress(split[0], Integer.parseInt(split[1]));
        nodes.add(dn);
      }
    }
  }

  private class ReadWithDNPreference extends RWWithDNPreferenceBase {
    ReadWithDNPreference() {
      super("debugRead",
          "debugRead [-favored DN1,DN2,..] [-excluded DN1,DN2,..] <HDFS path>",
          "  Test reading with favored/excluded DNs.");
    }

    @Override
    protected void processPath(String hdfsPath, List<String> leftoverArgs) throws IOException {
      Path path = new Path(hdfsPath);
      FSDataInputStream is = dfs.open(path);
      File file = new File(path.getName());
      ByteBuffer buff = ByteBuffer.allocate(4096);
      byte[] bufferContents = new byte[4096];
      long position = 0;
      OutputStream out = Files.newOutputStream(file.toPath());
      try {
        int res;
        while ((res = is.read(position, buff)) != -1) {
          buff.position(0);
          buff.get(bufferContents);
          buff.clear();
          out.write(bufferContents, 0, res);
          position += res;
        }
      } finally {
        is.close();
        out.close();
      }
    }
  }

  private class WriteWithDNPreference extends RWWithDNPreferenceBase {
    WriteWithDNPreference() {
      super("debugWrite",
          "debugWrite [-favored DN1,DN2,..] [-excluded DN1,DN2,..] <local file> <HDFS path>",
          "  Test reading with favored/excluded DNs.");
    }

    @Override
    protected void processPath(String hdfsPath, List<String> leftoverArgs) throws IOException {
      String localFile = leftoverArgs.get(0);
      File file = new File(localFile);
      Path path = new Path(hdfsPath);
      if (path.getName().equals(localFile)) {
        try {
          dfs.getFileStatus(path);
          throw new IOException("Destination already exists: " + path);
        } catch (FileNotFoundException ignored) {}
      } else {
        path = new Path(path, localFile);
      }
      FSDataOutputStream os = dfs.create(path);

      byte[] buff = new byte[4096];
      InputStream is = Files.newInputStream(file.toPath());
      try {
        int res;
        while ((res = is.read(buff)) != -1) {
          os.write(buff, 0, res);
        }
      } finally {
        is.close();
        os.flush();
        os.close();
      }
    }
  }

  /**
   * The command for getting help about other commands.
   */
  private class HelpCommand extends DebugCommand {
    HelpCommand() {
      super("help",
"help [command-name]",
"  Get help about a command.");
    }

    int run(List<String> args) {
      DebugCommand command = popCommand(args);
      if (command == null) {
        printUsage();
        return 0;
      }
      System.out.println(command.usageText);
      System.out.println(command.helpText + System.lineSeparator());
      return 0;
    }
  }

  public DebugAdmin(Configuration conf) {
    super(conf);
  }

  private DebugCommand popCommand(List<String> args) {
    String commandStr = (args.size() == 0) ? "" : args.get(0);
    if (commandStr.startsWith("-")) {
      commandStr = commandStr.substring(1);
    }
    for (DebugCommand command : DEBUG_COMMANDS) {
      if (command.name.equals(commandStr)) {
        args.remove(0);
        return command;
      }
    }
    return null;
  }

  public int run(String[] argv) {
    LinkedList<String> args = new LinkedList<String>();
    for (int j = 0; j < argv.length; ++j) {
      args.add(argv[j]);
    }
    DebugCommand command = popCommand(args);
    if (command == null) {
      printUsage();
      return 0;
    }
    try {
      return command.run(args);
    } catch (IOException e) {
      System.err.println("IOException: " +
          StringUtils.stringifyException(e));
      return 1;
    } catch (RuntimeException e) {
      System.err.println("RuntimeException: " +
          StringUtils.stringifyException(e));
      return 1;
    }
  }

  private void printUsage() {
    System.out.println("Usage: hdfs debug <command> [arguments]\n");
    System.out.println("These commands are for advanced users only.\n");
    System.out.println("Incorrect usages may result in data loss. " +
        "Use at your own risk.\n");
    for (DebugCommand command : DEBUG_COMMANDS) {
      if (!command.name.equals("help")) {
        System.out.println(command.usageText);
      }
      System.out.println();
      ToolRunner.printGenericCommandUsage(System.out);
    }
  }

  public static void main(String[] argsArray) throws Exception {
    DebugAdmin debugAdmin = new DebugAdmin(new Configuration());
    int res = ToolRunner.run(debugAdmin, argsArray);
    System.exit(res);
  }
}
