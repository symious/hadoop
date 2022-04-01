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
package org.apache.hadoop.hdfs;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BlockStoragePolicySpi;
import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.InvalidPathHandleException;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.PathHandle;
import org.apache.hadoop.fs.QuotaUsage;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.forward.ForwardCache;
import org.apache.hadoop.fs.forward.SchemeAndAuthority;
import org.apache.hadoop.fs.forward.UnderFSNotSupportedOperation;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.protocol.AddErasureCodingPolicyResponse;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsPathHandle;
import org.apache.hadoop.hdfs.protocol.OpenFileEntry;
import org.apache.hadoop.hdfs.protocol.OpenFilesIterator;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeInfo;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReportListing;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.protocol.ZoneReencryptionStatus;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.security.token.DelegationTokenIssuer;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public class ForwardDistributedFileSystem extends DistributedFileSystem {
  public static final Logger LOG = LoggerFactory.getLogger(
      ForwardDistributedFileSystem.class);

  private FileSystem underFs = null;
  private URI upperUri = null;
  private URI underUri = null;

  private boolean isForwardFileSystem = false;
  private SchemeAndAuthority upperSA;
  private SchemeAndAuthority underSA;

  private String getUncapPathString(final Path p) {
    return makeAbsolute(p).toUri().getPath();
  }

  private Path makeAbsolute(final Path f) {
    return f.isAbsolute() ? f : new Path(getWorkingDirectory(), f);
  }

  private Path floatPath(final Path p) {
    if (!isForwardFileSystem) {
      return p;
    }
    try {
      return new Path(new URI(upperSA.getScheme(), upperSA.getAuthority(),
          getUncapPathString(p), null, null));
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }
    return p;
  }

  private Path divePath(final Path p) {
    if (!isForwardFileSystem) {
      return p;
    }
    try {
      return new Path(new URI(underSA.getScheme(), underSA.getAuthority(),
          getUncapPathString(p), null, null));
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }
    return p;
  }

  private Path[] divePath(final Path[] p) {
    if (!isForwardFileSystem) {
      return p;
    }
    Path[] resPath = new Path[p.length];
    for (int i = 0; i < p.length; i++) {
      resPath[i] = divePath(p[i]);
    }
    return resPath;
  }

  private FileStatus diveFileStatus(FileStatus orig) throws IOException {
    if (!isForwardFileSystem) {
      return orig;
    }
    orig.setPath(divePath(orig.getPath()));
    return orig;
  }

  private FileStatus floatFileStatus(FileStatus orig) throws IOException {
    if (!isForwardFileSystem) {
      return orig;
    }
    orig.setPath(floatPath(orig.getPath()));
    return orig;
  }

  private FileStatus[] floatFileStatus(FileStatus[] orig) throws IOException {
    if (!isForwardFileSystem) {
      return orig;
    }
    FileStatus[] res = new FileStatus[orig.length];
    for (int i = 0; i < orig.length; i++) {
      res[i] = floatFileStatus(orig[i]);
    }
    return res;
  }

  public void initialize(final URI theUri, final Configuration conf)
      throws IOException{
    upperSA = new SchemeAndAuthority(theUri);
    underSA = ForwardCache.forward(upperSA, conf);
    if (!upperSA.equals(underSA)) {
      isForwardFileSystem = true;
    }
    try {
      if (isForwardFileSystem) {
        upperUri = upperSA.toUri();
        underUri = underSA.toUri();
        underFs = FileSystem.get(underSA.toUri(), conf);
        setConf(conf);
      } else {
        super.initialize(upperSA.toUri(), conf);
      }
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }
  }

  private boolean useUnderFs() {
    if (!isForwardFileSystem) {
      return false;
    }
    if (underSA.getScheme().equalsIgnoreCase("hdfs")) {
      return false;
    }
    return true;
  }

  @Override
  public Path getWorkingDirectory() {
    if (useUnderFs()) {
      return underFs.getWorkingDirectory();
    } else {
      return super.getWorkingDirectory();
    }
  }

  @Override
  public long getDefaultBlockSize() {
    if (useUnderFs()) {
      return underFs.getDefaultBlockSize();
    } else {
      return super.getDefaultBlockSize();
    }
  }

  @Override
  public short getDefaultReplication() {
    if (useUnderFs()) {
      return underFs.getDefaultReplication();
    } else {
      return super.getDefaultReplication();
    }
  }

  @Override
  public void setWorkingDirectory(Path dir) {
    if (useUnderFs()) {
      underFs.setWorkingDirectory(divePath(dir));
    } else {
      super.setWorkingDirectory(dir);
    }
  }

  @Override
  public Path getHomeDirectory() {
    if (useUnderFs()) {
      return floatPath(underFs.getHomeDirectory());
    } else {
      return super.getHomeDirectory();
    }
  }

  @Override
  public URI getUri() {
    if (useUnderFs()) {
      return upperUri;
    } else {
      return super.getUri();
    }
  }

  /**
   * Returns the hedged read metrics object for this client.
   *
   * @return object of DFSHedgedReadMetrics
   */
  public DFSHedgedReadMetrics getHedgedReadMetrics() {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri,
          "getHedgedReadMetrics()");
    } else {
      return super.getHedgedReadMetrics();
    }
  }

  /**
   * Returns the slowdatanodes cache metrics object for this client.
   *
   * @return object of DFSSlowDatanodeCacheMetrics
   */
  public DFSSlowDatanodeCacheMetrics getSlowDatanodeCacheMetrics() {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri,
          "getSlowDatanodeCacheMetrics()");
    } else {
      return super.getSlowDatanodeCacheMetrics();
    }
  }

  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus file, long start,
      long len) throws IOException {
    if (useUnderFs()) {
      return underFs.getFileBlockLocations(diveFileStatus(file), start, len);
    } else {
      return super.getFileBlockLocations(file, start, len);
    }
  }

  /**
   * The returned BlockLocation will have different formats for replicated
   * and erasure coded file.
   * Please refer to
   * {@link FileSystem#getFileBlockLocations(FileStatus, long, long)}
   * for more details.
   */
  @Override
  public BlockLocation[] getFileBlockLocations(Path p,
      final long start, final long len) throws IOException {
    if (useUnderFs()) {
      return underFs.getFileBlockLocations(divePath(p), start, len);
    } else {
      return super.getFileBlockLocations(p, start, len);
    }
  }

  @Override
  public void setVerifyChecksum(boolean verifyChecksum) {
    if (useUnderFs()) {
      underFs.setVerifyChecksum(verifyChecksum);
    } else {
      super.setVerifyChecksum(verifyChecksum);
    }
  }

  /**
   * Start the lease recovery of a file
   *
   * @param f a file
   * @return true if the file is already closed
   * @throws IOException if an error occurs
   */
  public boolean recoverLease(final Path f) throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri, "recoverLease(Path)");
    } else {
      return super.recoverLease(f);
    }
  }

  @Override
  public FSDataInputStream open(Path f, final int bufferSize)
      throws IOException {
    if (useUnderFs()) {
      return underFs.open(divePath(f), bufferSize);
    } else {
      return super.open(f, bufferSize);
    }
  }

  /**
   * Opens an FSDataInputStream with the indicated file ID extracted from
   * the {@link PathHandle}.
   * @param fd Reference to entity in this FileSystem.
   * @param bufferSize the size of the buffer to be used.
   * @throws InvalidPathHandleException If PathHandle constraints do not hold
   * @throws IOException On I/O errors
   */
  @Override
  public FSDataInputStream open(PathHandle fd, int bufferSize)
      throws IOException {
    if (useUnderFs()) {
      return underFs.open(fd, bufferSize);
    } else {
      return super.open(fd, bufferSize);
    }
  }

  /**
   * Create a handle to an HDFS file.
   * @param st HdfsFileStatus instance from NameNode
   * @param opts Standard handle arguments
   * @throws IllegalArgumentException If the FileStatus instance refers to a
   * directory, symlink, or another namesystem.
   * @throws UnsupportedOperationException If opts are not specified or both
   * data and location are not allowed to change.
   * @return A handle to the file.
   */
  @Override
  protected HdfsPathHandle createPathHandle(FileStatus st, Options.HandleOpt... opts) {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri,
          "createPathHandle(FileStatus, Options.HandleOpt...)");
    } else {
      return super.createPathHandle(st, opts);
    }
  }

  @Override
  public FSDataOutputStream append(Path f, final int bufferSize,
      final Progressable progress) throws IOException {
    if (useUnderFs()) {
      return underFs.append(divePath(f), bufferSize, progress);
    } else {
      return super.append(f, bufferSize, progress);
    }
  }

  /**
   * Append to an existing file (optional operation).
   *
   * @param f the existing file to be appended.
   * @param flag Flags for the Append operation. CreateFlag.APPEND is mandatory
   *          to be present.
   * @param bufferSize the size of the buffer to be used.
   * @param progress for reporting progress if it is not null.
   * @return Returns instance of {@link FSDataOutputStream}
   * @throws IOException
   */
  public FSDataOutputStream append(Path f, final EnumSet<CreateFlag> flag,
      final int bufferSize, final Progressable progress) throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri,
          "append(Path, EnumSet<CreateFlag>, int, Progressable)");
    } else {
      return super.append(f, flag, bufferSize, progress);
    }
  }

  /**
   * Append to an existing file (optional operation).
   *
   * @param f the existing file to be appended.
   * @param flag Flags for the Append operation. CreateFlag.APPEND is mandatory
   *          to be present.
   * @param bufferSize the size of the buffer to be used.
   * @param progress for reporting progress if it is not null.
   * @param favoredNodes Favored nodes for new blocks
   * @return Returns instance of {@link FSDataOutputStream}
   * @throws IOException
   */
  public FSDataOutputStream append(Path f, final EnumSet<CreateFlag> flag,
      final int bufferSize, final Progressable progress,
      final InetSocketAddress[] favoredNodes) throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri, "append(Path, " +
          "EnumSet<CreateFlag>, int, Progressable, InetSocketAddress[])");
    } else {
      return super.append(f, flag, bufferSize, progress, favoredNodes);
    }
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    if (useUnderFs()) {
      return underFs.create(divePath(f), permission, overwrite, bufferSize,
          replication, blockSize, progress);
    } else {
      return super.create(f, permission, overwrite, bufferSize, replication,
          blockSize, progress);
    }
  }

  /**
   * Same as
   * {@link #create(Path, FsPermission, boolean, int, short, long,
   * Progressable)} with the addition of favoredNodes that is a hint to
   * where the namenode should place the file blocks.
   * The favored nodes hint is not persisted in HDFS. Hence it may be honored
   * at the creation time only. And with favored nodes, blocks will be pinned
   * on the datanodes to prevent balancing move the block. HDFS could move the
   * blocks during replication, to move the blocks from favored nodes. A value
   * of null means no favored nodes for this create
   */
  public HdfsDataOutputStream create(final Path f,
      final FsPermission permission, final boolean overwrite,
      final int bufferSize, final short replication, final long blockSize,
      final Progressable progress, final InetSocketAddress[] favoredNodes)
      throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri, "create(Path, " +
          "FsPermission, boolean, int, short, long, Progressable, " +
          "InetSocketAddress[])");
    } else {
      return super.create(f, permission, overwrite, bufferSize, replication,
          blockSize, progress, favoredNodes);
    }
  }

  @Override
  public FSDataOutputStream create(final Path f, final FsPermission permission,
      final EnumSet<CreateFlag> cflags, final int bufferSize,
      final short replication, final long blockSize,
      final Progressable progress, final Options.ChecksumOpt checksumOpt)
      throws IOException {
    if (useUnderFs()) {
      return underFs.create(divePath(f), permission, cflags, bufferSize,
          replication, blockSize, progress, checksumOpt);
    } else {
      return super.create(f, permission, cflags, bufferSize, replication,
          blockSize, progress, checksumOpt);
    }
  }

  @Override
  protected HdfsDataOutputStream primitiveCreate(Path f,
      FsPermission absolutePermission, EnumSet<CreateFlag> flag, int bufferSize,
      short replication, long blockSize, Progressable progress,
      Options.ChecksumOpt checksumOpt) throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri, "primitiveCreate(" +
          "Path, FsPermission, EnumSet<CreateFlag>, int, short, long, " +
          "Progressable, Options.ChecksumOpt");
    } else {
      return super.primitiveCreate(f, absolutePermission, flag, bufferSize,
          replication, blockSize, progress, checksumOpt);
    }
  }

  /**
   * Same as create(), except fails if parent directory doesn't already exist.
   */
  @Override
  public FSDataOutputStream createNonRecursive(final Path f,
      final FsPermission permission, final EnumSet<CreateFlag> flag,
      final int bufferSize, final short replication, final long blockSize,
      final Progressable progress) throws IOException {
    if (useUnderFs()) {
      return underFs.createNonRecursive(divePath(f), permission, flag,
          bufferSize, replication, blockSize, progress);
    } else {
      return super.createNonRecursive(f, permission, flag, bufferSize,
          replication, blockSize, progress);
    }
  }

  @Override
  public boolean setReplication(Path src, final short replication)
      throws IOException {
    if (useUnderFs()) {
      return underFs.setReplication(divePath(src), replication);
    } else {
      return super.setReplication(src, replication);
    }
  }

  /**
   * Set the source path to the specified storage policy.
   *
   * @param src The source path referring to either a directory or a file.
   * @param policyName The name of the storage policy.
   */
  @Override
  public void setStoragePolicy(final Path src, final String policyName)
      throws IOException {
    if (useUnderFs()) {
      underFs.setStoragePolicy(divePath(src), policyName);
    } else {
      super.setStoragePolicy(src, policyName);
    }
  }

  @Override
  public void unsetStoragePolicy(final Path src)
      throws IOException {
    if (useUnderFs()) {
      underFs.unsetStoragePolicy(divePath(src));
    } else {
      super.unsetStoragePolicy(src);
    }
  }

  @Override
  public BlockStoragePolicySpi getStoragePolicy(Path path) throws IOException {
    if (useUnderFs()) {
      return underFs.getStoragePolicy(divePath(path));
    } else {
      return super.getStoragePolicy(path);
    }
  }

  @Override
  public Collection<BlockStoragePolicy> getAllStoragePolicies()
      throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri,
          "getAllStoragePolicies()");
    } else {
      return super.getAllStoragePolicies();
    }
  }

  /**
   * Returns number of bytes within blocks with future generation stamp. These
   * are bytes that will be potentially deleted if we forceExit from safe mode.
   *
   * @return number of bytes.
   */
  public long getBytesWithFutureGenerationStamps() throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri,
          "getBytesWithFutureGenerationStamps()");
    } else {
      return super.getBytesWithFutureGenerationStamps();
    }
  }

  /**
   * Deprecated. Prefer {@link FileSystem#getAllStoragePolicies()}
   * @throws IOException
   */
  @Deprecated
  public BlockStoragePolicy[] getStoragePolicies() throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri, "getStoragePolicies()");
    } else {
      return super.getStoragePolicies();
    }
  }

  /**
   * Move blocks from srcs to trg and delete srcs afterwards.
   * The file block sizes must be the same.
   *
   * @param trg existing file to append to
   * @param psrcs list of files (same block size, same replication)
   * @throws IOException
   */
  @Override
  public void concat(Path trg, Path [] psrcs) throws IOException {
    if (useUnderFs()) {
      underFs.concat(divePath(trg), divePath(psrcs));
    } else {
      super.concat(trg, psrcs);
    }
  }


  @SuppressWarnings("deprecation")
  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    if (useUnderFs()) {
      return underFs.rename(divePath(src), divePath(dst));
    } else {
      return super.rename(src, dst);
    }
  }

  /**
   * This rename operation is guaranteed to be atomic.
   */
  @SuppressWarnings("deprecation")
  @Override
  public void rename(Path src, Path dst, final Options.Rename... options)
      throws IOException {
    if (useUnderFs()) {
      super.renameBasicFileSystem(src, dst, options);
    } else {
      super.rename(src, dst, options);
    }
  }

  @Override
  public boolean truncate(Path f, final long newLength) throws IOException {
    if (useUnderFs()) {
      return underFs.truncate(divePath(f), newLength);
    } else {
      return super.truncate(f, newLength);
    }
  }

  @Override
  public boolean delete(Path f, final boolean recursive) throws IOException {
    if (useUnderFs()) {
      return underFs.delete(divePath(f), recursive);
    } else {
      return super.delete(f, recursive);
    }
  }

  @Override
  public ContentSummary getContentSummary(Path f) throws IOException {
    if (useUnderFs()) {
      return underFs.getContentSummary(divePath(f));
    } else {
      return super.getContentSummary(f);
    }
  }

  @Override
  public QuotaUsage getQuotaUsage(Path f) throws IOException {
    if (useUnderFs()) {
      return underFs.getQuotaUsage(divePath(f));
    } else {
      return super.getQuotaUsage(f);
    }
  }

  /** Set a directory's quotas
   * @see org.apache.hadoop.hdfs.protocol.ClientProtocol#setQuota(String,
   * long, long, StorageType)
   */
  public void setQuota(Path src, final long namespaceQuota,
      final long storagespaceQuota) throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri, "setQuota(Path, long, " +
          "long)");
    } else {
      super.setQuota(src, namespaceQuota, storagespaceQuota);
    }
  }

  /**
   * Set the per type storage quota of a directory.
   *
   * @param src target directory whose quota is to be modified.
   * @param type storage type of the specific storage type quota to be modified.
   * @param quota value of the specific storage type quota to be modified.
   * Maybe {@link HdfsConstants#QUOTA_RESET} to clear quota by storage type.
   */
  public void setQuotaByStorageType(Path src, final StorageType type,
      final long quota)
      throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri, "setQuotaByStorageType" +
          "(Path, StorageType, long)");
    } else {
      super.setQuotaByStorageType(src, type, quota);
    }
  }

  /**
   * List all the entries of a directory
   *
   * Note that this operation is not atomic for a large directory.
   * The entries of a directory may be fetched from NameNode multiple times.
   * It only guarantees that  each name occurs once if a directory
   * undergoes changes between the calls.
   */
  @Override
  public FileStatus[] listStatus(Path p) throws IOException {
    if (useUnderFs()) {
      return floatFileStatus(underFs.listStatus(divePath(p)));
    } else {
      return super.listStatus(p);
    }
  }

  /**
   * The BlockLocation of returned LocatedFileStatus will have different
   * formats for replicated and erasure coded file.
   * Please refer to
   * {@link FileSystem#getFileBlockLocations(FileStatus, long, long)} for
   * more details.
   */
  @Override
  protected RemoteIterator<LocatedFileStatus> listLocatedStatus(final Path p,
      final PathFilter filter)
      throws IOException {
    if (useUnderFs()) {
      return new RemoteIterator<LocatedFileStatus>() {
        private final FileStatus[] stats =
            floatFileStatus(listStatus(divePath(p), filter));
        private int i = 0;

        @Override
        public boolean hasNext() {
          return i<stats.length;
        }

        @Override
        public LocatedFileStatus next() throws IOException {
          if (!hasNext()) {
            throw new NoSuchElementException("No more entries in " + p);
          }
          FileStatus result = stats[i++];
          // for files, use getBlockLocations(FileStatus, int, int) to avoid
          // calling getFileStatus(Path) to load the FileStatus again
          BlockLocation[] locs = result.isFile() ?
              getFileBlockLocations(result, 0, result.getLen()) :
              null;
          return new LocatedFileStatus(floatFileStatus(result), locs);
        }
      };
    } else {
      return super.listLocatedStatus(p, filter);
    }
  }


  /**
   * Returns a remote iterator so that followup calls are made on demand
   * while consuming the entries. This reduces memory consumption during
   * listing of a large directory.
   *
   * @param p target path
   * @return remote iterator
   */
  @Override
  public RemoteIterator<FileStatus> listStatusIterator(final Path p)
      throws IOException {
    if (useUnderFs()) {
      final RemoteIterator<FileStatus> fsIter =
          underFs.listStatusIterator(divePath(p));
      return new RemoteIterator<FileStatus>() {
        @Override
        public boolean hasNext() throws IOException {
          return fsIter.hasNext();
        }

        @Override
        public FileStatus next() throws IOException {
          final FileStatus status = fsIter.next();
          return floatFileStatus(status);
        }
      };
    } else {
      return super.listStatusIterator(p);
    }
  }

  /**
   * Create a directory, only when the parent directories exist.
   *
   * See {@link FsPermission#applyUMask(FsPermission)} for details of how
   * the permission is applied.
   *
   * @param f           The path to create
   * @param permission  The permission.  See FsPermission#applyUMask for
   *                    details about how this is used to calculate the
   *                    effective permission.
   */
  public boolean mkdir(Path f, FsPermission permission) throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri, "mkdir(Path, " +
          "FsPermission)");
    } else {
      return super.mkdir(f, permission);
    }
  }

  /**
   * Create a directory and its parent directories.
   *
   * See {@link FsPermission#applyUMask(FsPermission)} for details of how
   * the permission is applied.
   *
   * @param f           The path to create
   * @param permission  The permission.  See FsPermission#applyUMask for
   *                    details about how this is used to calculate the
   *                    effective permission.
   */
  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    if (useUnderFs()) {
      return underFs.mkdirs(divePath(f), permission);
    } else {
      return super.mkdirs(f, permission);
    }
  }

  @SuppressWarnings("deprecation")
  @Override
  protected boolean primitiveMkdir(Path f, FsPermission absolutePermission)
      throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri, "primitiveMkdir(Path, " +
          "FsPermission)");
    } else {
      return super.primitiveMkdir(f, absolutePermission);
    }
  }


  @Override
  public void close() throws IOException {
    if (useUnderFs()) {
      underFs.close();
      closeBasicFileSystem();
    } else {
      super.close();
    }
  }

  @Override
  public String toString() {
    if (useUnderFs()) {
      return "ForwardDFS[" + underFs.toString() + "]";
    } else {
      return "ForwardDFS[" + super.toString() + "]";
    }
  }

  @InterfaceAudience.Private
  @VisibleForTesting
  public DFSClient getClient() {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri, "getClient()");
    } else {
      return super.getClient();
    }
  }

  @Override
  public FsStatus getStatus(Path p) throws IOException {
    if (useUnderFs()) {
      return underFs.getStatus(divePath(p));
    } else {
      return super.getStatus(p);
    }
  }

  /**
   * Returns count of blocks with no good replicas left. Normally should be
   * zero.
   *
   * @throws IOException
   */
  public long getMissingBlocksCount() throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri,
          "getMissingBlocksCount()");
    } else {
      return super.getMissingBlocksCount();
    }
  }

  /**
   * Returns count of blocks pending on deletion.
   *
   * @throws IOException
   */
  public long getPendingDeletionBlocksCount() throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri,
          "getPendingDeletionBlocksCount()");
    } else {
      return super.getPendingDeletionBlocksCount();
    }
  }

  /**
   * Returns count of blocks with replication factor 1 and have
   * lost the only replica.
   *
   * @throws IOException
   */
  public long getMissingReplOneBlocksCount() throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri,
          "getMissingReplOneBlocksCount()");
    } else {
      return super.getMissingReplOneBlocksCount();
    }
  }

  /**
   * Returns aggregated count of blocks with less redundancy.
   *
   * @throws IOException
   */
  public long getLowRedundancyBlocksCount() throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri,
          "getLowRedundancyBlocksCount()");
    } else {
      return super.getLowRedundancyBlocksCount();
    }
  }

  /**
   * Returns count of blocks with at least one replica marked corrupt.
   *
   * @throws IOException
   */
  public long getCorruptBlocksCount() throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri,
          "getCorruptBlocksCount()");
    } else {
      return super.getCorruptBlocksCount();
    }
  }

  @Override
  public RemoteIterator<Path> listCorruptFileBlocks(final Path path)
      throws IOException {
    if (useUnderFs()) {
      final RemoteIterator<Path> pathIter =
          underFs.listCorruptFileBlocks(divePath(path));
      return new RemoteIterator<Path>() {
        @Override
        public boolean hasNext() throws IOException {
          return pathIter.hasNext();
        }

        @Override
        public Path next() throws IOException {
          return floatPath(pathIter.next());
        }
      };
    } else {
      return super.listCorruptFileBlocks(path);
    }
  }

  /** @return datanode statistics. */
  public DatanodeInfo[] getDataNodeStats() throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri, "getDataNodeStats()");
    } else {
      return super.getDataNodeStats();
    }
  }

  /** @return datanode statistics for the given type. */
  public DatanodeInfo[] getDataNodeStats(final HdfsConstants.DatanodeReportType type)
      throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri,
          "getDataNodeStats(DatanodeReportType)");
    } else {
      return super.getDataNodeStats(type);
    }
  }

  /**
   * Enter, leave or get safe mode.
   *
   * @see org.apache.hadoop.hdfs.protocol.ClientProtocol#setSafeMode(
   *    HdfsConstants.SafeModeAction,boolean)
   */
  public boolean setSafeMode(HdfsConstants.SafeModeAction action)
      throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri,
          "setSafeMode(SafeModeAction)");
    } else {
      return super.setSafeMode(action);
    }
  }

  /**
   * Enter, leave or get safe mode.
   *
   * @param action
   *          One of SafeModeAction.ENTER, SafeModeAction.LEAVE and
   *          SafeModeAction.GET
   * @param isChecked
   *          If true check only for Active NNs status, else check first NN's
   *          status
   * @see org.apache.hadoop.hdfs.protocol.ClientProtocol#setSafeMode(HdfsConstants.SafeModeAction, boolean)
   */
  public boolean setSafeMode(HdfsConstants.SafeModeAction action,
      boolean isChecked) throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri,
          "setSafeMode(SafeModeAction, boolean)");
    } else {
      return super.setSafeMode(action, isChecked);
    }
  }

  /**
   * Save namespace image.
   *
   * @param timeWindow NameNode can ignore this command if the latest
   *                   checkpoint was done within the given time period (in
   *                   seconds).
   * @return true if a new checkpoint has been made
   * @see ClientProtocol#saveNamespace(long, long)
   */
  public boolean saveNamespace(long timeWindow, long txGap) throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri,
          "saveNamespace(long, long)");
    } else {
      return super.saveNamespace(timeWindow, txGap);
    }
  }

  /**
   * Save namespace image. NameNode always does the checkpoint.
   */
  public void saveNamespace() throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri, "saveNamespace()");
    } else {
      super.saveNamespace();
    }
  }

  /**
   * Rolls the edit log on the active NameNode.
   * Requires super-user privileges.
   * @see org.apache.hadoop.hdfs.protocol.ClientProtocol#rollEdits()
   * @return the transaction ID of the newly created segment
   */
  public long rollEdits() throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri, "rollEdits()");
    } else {
      return super.rollEdits();
    }
  }

  /**
   * enable/disable/check restoreFaileStorage
   *
   * @see org.apache.hadoop.hdfs.protocol.ClientProtocol#restoreFailedStorage(String arg)
   */
  public boolean restoreFailedStorage(String arg) throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri,
          "restoreFailedStorage(String)");
    } else {
      return super.restoreFailedStorage(arg);
    }
  }


  /**
   * Refreshes the list of hosts and excluded hosts from the configured
   * files.
   */
  public void refreshNodes() throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri, "refreshNodes()");
    } else {
      super.refreshNodes();
    }
  }

  /**
   * Finalize previously upgraded files system state.
   * @throws IOException
   */
  public void finalizeUpgrade() throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri, "finalizeUpgrade()");
    } else {
      super.finalizeUpgrade();
    }
  }

  /**
   * Get status of upgrade - finalized or not.
   * @return true if upgrade is finalized or if no upgrade is in progress and
   * false otherwise.
   * @throws IOException
   */
  public boolean upgradeStatus() throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri, "upgradeStatus()");
    } else {
      return super.upgradeStatus();
    }
  }

  /**
   * Rolling upgrade: prepare/finalize/query.
   */
  public RollingUpgradeInfo rollingUpgrade(
      HdfsConstants.RollingUpgradeAction action)
      throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri,
          "rollingUpgrade(RollingUpgradeAction)");
    } else {
      return super.rollingUpgrade(action);
    }
  }

  /*
   * Requests the namenode to dump data strcutures into specified
   * file.
   */
  public void metaSave(String pathname) throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri, "metaSave(String)");
    } else {
      super.metaSave(pathname);
    }
  }

  @Override
  public FsServerDefaults getServerDefaults() throws IOException {
    if (useUnderFs()) {
      return underFs.getServerDefaults();
    } else {
      return super.getServerDefaults();
    }
  }

  /**
   * Returns the stat information about the file.
   * @throws FileNotFoundException if the file does not exist.
   */
  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    if (useUnderFs()) {
      return floatFileStatus(underFs.getFileStatus(divePath(f)));
    } else {
      return super.getFileStatus(f);
    }
  }

  @SuppressWarnings("deprecation")
  @Override
  public void createSymlink(final Path target, final Path link,
      final boolean createParent) throws IOException {
    if (useUnderFs()) {
      underFs.createSymlink(divePath(target), divePath(link), createParent);
    } else {
      super.createSymlink(target, link, createParent);
    }
  }

  @Override
  public boolean supportsSymlinks() {
    if (useUnderFs()) {
      return underFs.supportsSymlinks();
    } else {
      return super.supportsSymlinks();
    }
  }

  @Override
  public FileStatus getFileLinkStatus(final Path f) throws IOException {
    if (useUnderFs()) {
      return floatFileStatus(underFs.getFileLinkStatus(divePath(f)));
    } else {
      return super.getFileLinkStatus(f);
    }
  }

  @Override
  public Path getLinkTarget(final Path f) throws IOException {
    if (useUnderFs()) {
      return floatPath(underFs.getLinkTarget(divePath(f)));
    } else {
      return super.getLinkTarget(f);
    }
  }

  @Override
  protected Path resolveLink(Path f) throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri, "resolveLink(Path)");
    } else {
      return super.resolveLink(f);
    }
  }

  @Override
  public FileChecksum getFileChecksum(Path f) throws IOException {
    if (useUnderFs()) {
      return underFs.getFileChecksum(divePath(f));
    } else {
      return super.getFileChecksum(f);
    }
  }

  @Override
  public FileChecksum getFileChecksum(Path f, final long length)
      throws IOException {
    if (useUnderFs()) {
      return underFs.getFileChecksum(divePath(f), length);
    } else {
      return super.getFileChecksum(f, length);
    }
  }

  @Override
  public void setPermission(Path p, final FsPermission permission
  ) throws IOException {
    if (useUnderFs()) {
      underFs.setPermission(divePath(p), permission);
    } else {
      super.setPermission(p, permission);
    }
  }

  @Override
  public void setOwner(Path p, final String username, final String groupname)
      throws IOException {
    if (useUnderFs()) {
      underFs.setOwner(divePath(p), username, groupname);
    } else {
      super.setOwner(p, username, groupname);
    }
  }

  @Override
  public void setTimes(Path p, final long mtime, final long atime)
      throws IOException {
    if (useUnderFs()) {
      underFs.setTimes(divePath(p), mtime, atime);
    } else {
      super.setTimes(p, mtime, atime);
    }
  }


  @Override
  protected int getDefaultPort() {
    if (useUnderFs()) {
      return 0;
    } else {
      return super.getDefaultPort();
    }
  }

  @Override
  public Token<DelegationTokenIdentifier> getDelegationToken(String renewer)
      throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri,
          "getDelegationToken(String)");
    } else {
      return super.getDelegationToken(renewer);
    }
  }

  /**
   * Requests the namenode to tell all datanodes to use a new, non-persistent
   * bandwidth value for dfs.datanode.balance.bandwidthPerSec.
   * The bandwidth parameter is the max number of bytes per second of network
   * bandwidth to be used by a datanode during balancing.
   *
   * @param bandwidth Balancer bandwidth in bytes per second for all datanodes.
   * @throws IOException
   */
  public void setBalancerBandwidth(long bandwidth) throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri,
          "setBalancerBandwidth(long)");
    } else {
      super.setBalancerBandwidth(bandwidth);
    }
  }

  /**
   * Get a canonical service name for this file system. If the URI is logical,
   * the hostname part of the URI will be returned.
   * @return a service string that uniquely identifies this file system.
   */
  @Override
  public String getCanonicalServiceName() {
    if (useUnderFs()) {
      return underFs.getCanonicalServiceName();
    } else {
      return super.getCanonicalServiceName();
    }
  }

  @Override
  protected URI canonicalizeUri(URI uri) {
    if (useUnderFs()) {
      if (uri.getPort() == -1 && getDefaultPort() > 0) {
        // reconstruct the uri with the default port set
        try {
          uri = new URI(uri.getScheme(), uri.getUserInfo(),
              uri.getHost(), getDefaultPort(),
              uri.getPath(), uri.getQuery(), uri.getFragment());
        } catch (URISyntaxException e) {
          // Should never happen!
          throw new AssertionError("Valid URI became unparseable: " +
              uri);
        }
      }
      return uri;
    } else {
      return super.canonicalizeUri(uri);
    }
  }

  /**
   * Utility function that returns if the NameNode is in safemode or not. In HA
   * mode, this API will return only ActiveNN's safemode status.
   *
   * @return true if NameNode is in safemode, false otherwise.
   * @throws IOException
   *           when there is an issue communicating with the NameNode
   */
  public boolean isInSafeMode() throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri, "isInSafeMode()");
    } else {
      return super.isInSafeMode();
    }
  }

  /** @see org.apache.hadoop.hdfs.client.HdfsAdmin#allowSnapshot(Path) */
  public void allowSnapshot(final Path path) throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri, "allowSnapshot(Path)");
    } else {
      super.allowSnapshot(path);
    }
  }

  /** @see org.apache.hadoop.hdfs.client.HdfsAdmin#disallowSnapshot(Path) */
  public void disallowSnapshot(final Path path) throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri,
          "disallowSnapshot(Path)");
    } else {
      super.disallowSnapshot(path);
    }
  }

  @Override
  public Path createSnapshot(final Path path, final String snapshotName)
      throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri,
          "createSnapshot(Path, String)");
    } else {
      return super.createSnapshot(path, snapshotName);
    }
  }

  @Override
  public void renameSnapshot(final Path path, final String snapshotOldName,
      final String snapshotNewName) throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri,
          "renameSnapshot(Path, String, String)");
    } else {
      super.renameSnapshot(path, snapshotOldName, snapshotNewName);
    }
  }

  /**
   * @return All the snapshottable directories
   * @throws IOException
   */
  public SnapshottableDirectoryStatus[] getSnapshottableDirListing()
      throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri,
          "getSnapshottableDirListing()");
    } else {
      return super.getSnapshottableDirListing();
    }
  }

  @Override
  public void deleteSnapshot(final Path snapshotDir, final String snapshotName)
      throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri,
          "deleteSnapshot(Path, String)");
    } else {
      super.deleteSnapshot(snapshotDir, snapshotName);
    }
  }

  /**
   * Returns a remote iterator so that followup calls are made on demand
   * while consuming the SnapshotDiffReportListing entries.
   * This reduces memory consumption overhead in case the snapshotDiffReport
   * is huge.
   *
   * @param snapshotDir
   *          full path of the directory where snapshots are taken
   * @param fromSnapshot
   *          snapshot name of the from point. Null indicates the current
   *          tree
   * @param toSnapshot
   *          snapshot name of the to point. Null indicates the current
   *          tree.
   * @return Remote iterator
   */
  public RemoteIterator
      <SnapshotDiffReportListing> snapshotDiffReportListingRemoteIterator(
      final Path snapshotDir, final String fromSnapshot,
      final String toSnapshot) throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri,
          "snapshotDiffReportListingRemoteIterator(Path, String, String)");
    } else {
      return super.snapshotDiffReportListingRemoteIterator(snapshotDir,
          fromSnapshot, toSnapshot);
    }
  }

  /**
   * Get the difference between two snapshots, or between a snapshot and the
   * current tree of a directory.
   *
   * @see DFSClient#getSnapshotDiffReportListing
   */
  public SnapshotDiffReport getSnapshotDiffReport(final Path snapshotDir,
      final String fromSnapshot, final String toSnapshot) throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri,
          "getSnapshotDiffReport(Path, String, String)");
    } else {
      return super.getSnapshotDiffReport(snapshotDir, fromSnapshot, toSnapshot);
    }
  }

  /**
   * Get the close status of a file
   * @param src The path to the file
   *
   * @return return true if file is closed
   * @throws FileNotFoundException if the file does not exist.
   * @throws IOException If an I/O error occurred
   */
  public boolean isFileClosed(final Path src) throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri, "isFileClosed(Path)");
    } else {
      return super.isFileClosed(src);
    }
  }

  /**
   * @see #addCacheDirective(CacheDirectiveInfo, EnumSet)
   */
  public long addCacheDirective(CacheDirectiveInfo info) throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri,
          "addCacheDirective(CacheDirectiveInfo)");
    } else {
      return super.addCacheDirective(info);
    }
  }

  /**
   * Add a new CacheDirective.
   *
   * @param info Information about a directive to add.
   * @param flags {@link CacheFlag}s to use for this operation.
   * @return the ID of the directive that was created.
   * @throws IOException if the directive could not be added
   */
  public long addCacheDirective(
      CacheDirectiveInfo info, EnumSet<CacheFlag> flags) throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri,
          "addCacheDirective(CacheDirectiveInfo, EnumSet<CacheFlag>)");
    } else {
      return super.addCacheDirective(info, flags);
    }
  }

  /**
   * @see #modifyCacheDirective(CacheDirectiveInfo, EnumSet)
   */
  public void modifyCacheDirective(CacheDirectiveInfo info) throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri,
          "modifyCacheDirective(CacheDirectiveInfo)");
    } else {
      super.modifyCacheDirective(info);
    }
  }

  /**
   * Modify a CacheDirective.
   *
   * @param info Information about the directive to modify. You must set the ID
   *          to indicate which CacheDirective you want to modify.
   * @param flags {@link CacheFlag}s to use for this operation.
   * @throws IOException if the directive could not be modified
   */
  public void modifyCacheDirective(
      CacheDirectiveInfo info, EnumSet<CacheFlag> flags) throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri,
          "modifyCacheDirective(CacheDirectiveInfo, EnumSet<CacheFlag>)");
    } else {
      super.modifyCacheDirective(info, flags);
    }
  }

  /**
   * Remove a CacheDirectiveInfo.
   *
   * @param id identifier of the CacheDirectiveInfo to remove
   * @throws IOException if the directive could not be removed
   */
  public void removeCacheDirective(long id)
      throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri,
          "removeCacheDirective(long)");
    } else {
      super.removeCacheDirective(id);
    }
  }

  /**
   * List cache directives.  Incrementally fetches results from the server.
   *
   * @param filter Filter parameters to use when listing the directives, null to
   *               list all directives visible to us.
   * @return A RemoteIterator which returns CacheDirectiveInfo objects.
   */
  public RemoteIterator<CacheDirectiveEntry> listCacheDirectives(
      CacheDirectiveInfo filter) throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri,
          "listCacheDirectives(CacheDirectiveInfo)");
    } else {
      return super.listCacheDirectives(filter);
    }
  }

  /**
   * Add a cache pool.
   *
   * @param info
   *          The request to add a cache pool.
   * @throws IOException
   *          If the request could not be completed.
   */
  public void addCachePool(CachePoolInfo info) throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri,
          "addCachePool(CachePoolInfo)");
    } else {
      super.addCachePool(info);
    }
  }

  /**
   * Modify an existing cache pool.
   *
   * @param info
   *          The request to modify a cache pool.
   * @throws IOException
   *          If the request could not be completed.
   */
  public void modifyCachePool(CachePoolInfo info) throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri,
          "modifyCachePool(CachePoolInfo)");
    } else {
      super.modifyCachePool(info);
    }
  }

  /**
   * Remove a cache pool.
   *
   * @param poolName
   *          Name of the cache pool to remove.
   * @throws IOException
   *          if the cache pool did not exist, or could not be removed.
   */
  public void removeCachePool(String poolName) throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri,
          "removeCachePool(String)");
    } else {
      super.removeCachePool(poolName);
    }
  }

  /**
   * List all cache pools.
   *
   * @return A remote iterator from which you can get CachePoolEntry objects.
   *          Requests will be made as needed.
   * @throws IOException
   *          If there was an error listing cache pools.
   */
  public RemoteIterator<CachePoolEntry> listCachePools() throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri, "listCachePools()");
    } else {
      return super.listCachePools();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void modifyAclEntries(Path path, final List<AclEntry> aclSpec)
      throws IOException {
    if (useUnderFs()) {
      underFs.modifyAclEntries(divePath(path), aclSpec);
    } else {
      super.modifyAclEntries(path, aclSpec);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void removeAclEntries(Path path, final List<AclEntry> aclSpec)
      throws IOException {
    if (useUnderFs()) {
      underFs.removeAclEntries(divePath(path), aclSpec);
    } else {
      super.removeAclEntries(path, aclSpec);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void removeDefaultAcl(Path path) throws IOException {
    if (useUnderFs()) {
      underFs.removeDefaultAcl(divePath(path));
    } else {
      super.removeDefaultAcl(path);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void removeAcl(Path path) throws IOException {
    if (useUnderFs()) {
      underFs.removeAcl(divePath(path));
    } else {
      super.removeAcl(path);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setAcl(Path path, final List<AclEntry> aclSpec)
      throws IOException {
    if (useUnderFs()) {
      underFs.setAcl(divePath(path), aclSpec);
    } else {
      super.setAcl(path, aclSpec);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public AclStatus getAclStatus(Path path) throws IOException {
    if (useUnderFs()) {
      try {
        return underFs.getAclStatus(divePath(path));
      } catch (UnsupportedOperationException e) {
        return null;
      }
    } else {
      return super.getAclStatus(path);
    }
  }

  /* HDFS only */
  public void createEncryptionZone(final Path path, final String keyName)
      throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri,
          "createEncryptionZone(Path, String)");
    } else {
      super.createEncryptionZone(path, keyName);
    }
  }

  /* HDFS only */
  public EncryptionZone getEZForPath(final Path path)
      throws IOException {
    if (useUnderFs()) {
      // Usually return null indicates FS is not encrypted.
      return null;
    } else {
      return super.getEZForPath(path);
    }
  }

  /* HDFS only */
  public RemoteIterator<EncryptionZone> listEncryptionZones()
      throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri, "listEncryptionZones()");
    } else {
      return super.listEncryptionZones();
    }
  }

  /* HDFS only */
  public void reencryptEncryptionZone(final Path zone,
      final HdfsConstants.ReencryptAction action) throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri,
          "reencryptENcryptionZone(Path, ReencryptAction)");
    } else {
      super.reencryptEncryptionZone(zone, action);
    }
  }

  /* HDFS only */
  public RemoteIterator<ZoneReencryptionStatus> listReencryptionStatus()
      throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri,
          "listReencryptionStatus()");
    } else {
      return super.listReencryptionStatus();
    }
  }

  /* HDFS only */
  public FileEncryptionInfo getFileEncryptionInfo(final Path path)
      throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri,
          "getFileEncryptionInfo(Path)");
    } else {
      return super.getFileEncryptionInfo(path);
    }
  }

  /* HDFS only */
  public void provisionEZTrash(final Path path,
      final FsPermission trashPermission) throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri,
          "provisionEZTrash(Path, FsPermission)");
    } else {
      super.provisionEZTrash(path, trashPermission);
    }
  }

  @Override
  public void setXAttr(Path path, final String name, final byte[] value,
      final EnumSet<XAttrSetFlag> flag) throws IOException {
    if (useUnderFs()) {
      underFs.setXAttr(divePath(path), name, value, flag);
    } else {
      super.setXAttr(path, name, value, flag);
    }
  }

  @Override
  public byte[] getXAttr(Path path, final String name) throws IOException {
    if (useUnderFs()) {
      return underFs.getXAttr(divePath(path), name);
    } else {
      return super.getXAttr(path, name);
    }
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path path) throws IOException {
    if (useUnderFs()) {
      return underFs.getXAttrs(divePath(path));
    } else {
      return super.getXAttrs(path);
    }
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path path, final List<String> names)
      throws IOException {
    if (useUnderFs()) {
      return underFs.getXAttrs(divePath(path), names);
    } else {
      return super.getXAttrs(path, names);
    }
  }

  @Override
  public List<String> listXAttrs(Path path)
      throws IOException {
    if (useUnderFs()) {
      return underFs.listXAttrs(divePath(path));
    } else {
      return super.listXAttrs(path);
    }
  }

  @Override
  public void removeXAttr(Path path, final String name) throws IOException {
    if (useUnderFs()) {
      underFs.removeXAttr(divePath(path), name);
    } else {
      super.removeXAttr(path, name);
    }
  }

  @Override
  public void access(Path path, final FsAction mode) throws IOException {
    if (useUnderFs()) {
      underFs.access(divePath(path), mode);
    } else {
      super.access(path, mode);
    }
  }

  @Override
  public URI getKeyProviderUri() throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri, "getKeyProviderUri()");
    } else {
      return super.getKeyProviderUri();
    }
  }

  @Override
  public KeyProvider getKeyProvider() throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri, "getKeyProvider()");
    } else {
      return super.getKeyProvider();
    }
  }

  @Override
  public DelegationTokenIssuer[] getAdditionalTokenIssuers()
      throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri,
          "getAdditionalTokenIssuers()");
    } else {
      return super.getAdditionalTokenIssuers();
    }
  }

  public DFSInotifyEventInputStream getInotifyEventStream() throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri,
          "getInotifyEventStream()");
    } else {
      return super.getInotifyEventStream();
    }
  }

  public DFSInotifyEventInputStream getInotifyEventStream(long lastReadTxid)
      throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri,
          "getInotifyEventStream(long)");
    } else {
      return super.getInotifyEventStream(lastReadTxid);
    }
  }

  /**
   * Set the source path to the specified erasure coding policy.
   *
   * @param path     The directory to set the policy
   * @param ecPolicyName The erasure coding policy name.
   * @throws IOException
   */
  public void setErasureCodingPolicy(final Path path,
      final String ecPolicyName) throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri,
          "setErasureCodingPolicy(Path, String)");
    } else {
      super.setErasureCodingPolicy(path, ecPolicyName);
    }
  }

  /**
   * Set the source path to satisfy storage policy. This API is non-recursive
   * in nature, i.e., if the source path is a directory then all the files
   * immediately under the directory would be considered for satisfying the
   * policy and the sub-directories if any under this path will be skipped.
   *
   * @param path The source path referring to either a directory or a file.
   * @throws IOException
   */
  public void satisfyStoragePolicy(final Path path) throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri,
          "satisfyStoragePolicy(Path)");
    } else {
      super.satisfyStoragePolicy(path);
    }
  }

  /**
   * Get erasure coding policy information for the specified path
   *
   * @param path The path of the file or directory
   * @return Returns the policy information if file or directory on the path
   * is erasure coded, null otherwise. Null will be returned if directory or
   * file has REPLICATION policy.
   * @throws IOException
   */
  public ErasureCodingPolicy getErasureCodingPolicy(final Path path)
      throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri,
          "getErasureCodingPolicy(Path)");
    } else {
      return super.getErasureCodingPolicy(path);
    }
  }

  /**
   * Retrieve all the erasure coding policies supported by this file system,
   * including enabled, disabled and removed policies, but excluding
   * REPLICATION policy.
   *
   * @return all erasure coding policies supported by this file system.
   * @throws IOException
   */
  public Collection<ErasureCodingPolicyInfo> getAllErasureCodingPolicies()
      throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri,
          "getAllErasureCodingPolicies()");
    } else {
      return super.getAllErasureCodingPolicies();
    }
  }

  /**
   * Retrieve all the erasure coding codecs and coders supported by this file
   * system.
   *
   * @return all erasure coding codecs and coders supported by this file system.
   * @throws IOException
   */
  public Map<String, String> getAllErasureCodingCodecs()
      throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri,
          "getAllErasureCodingCodecs()");
    } else {
      return super.getAllErasureCodingCodecs();
    }
  }

  /**
   * Add Erasure coding policies to HDFS. For each policy input, schema and
   * cellSize are musts, name and id are ignored. They will be automatically
   * created and assigned by Namenode once the policy is successfully added,
   * and will be returned in the response; policy states will be set to
   * DISABLED automatically.
   *
   * @param policies The user defined ec policy list to add.
   * @return Return the response list of adding operations.
   * @throws IOException
   */
  public AddErasureCodingPolicyResponse[] addErasureCodingPolicies(
      ErasureCodingPolicy[] policies)  throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri,
          "addErasureCodingPolicies(ErasureCodingPolicy[])");
    } else {
      return super.addErasureCodingPolicies(policies);
    }
  }

  /**
   * Remove erasure coding policy.
   *
   * @param ecPolicyName The name of the policy to be removed.
   * @throws IOException
   */
  public void removeErasureCodingPolicy(String ecPolicyName)
      throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri,
          "removeErasureCodingPolicy(String)");
    } else {
      super.removeErasureCodingPolicy(ecPolicyName);
    }
  }

  /**
   * Enable erasure coding policy.
   *
   * @param ecPolicyName The name of the policy to be enabled.
   * @throws IOException
   */
  public void enableErasureCodingPolicy(String ecPolicyName)
      throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri,
          "enableErasureCodingPolicy(String)");
    } else {
      super.enableErasureCodingPolicy(ecPolicyName);
    }
  }

  /**
   * Disable erasure coding policy.
   *
   * @param ecPolicyName The name of the policy to be disabled.
   * @throws IOException
   */
  public void disableErasureCodingPolicy(String ecPolicyName)
      throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri,
          "disableErasureCodingPolicy(String)");
    } else {
      super.disableErasureCodingPolicy(ecPolicyName);
    }
  }

  /**
   * Unset the erasure coding policy from the source path.
   *
   * @param path     The directory to unset the policy
   * @throws IOException
   */
  public void unsetErasureCodingPolicy(final Path path) throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri,
          "unsetErasureCodingPolicy(Path)");
    } else {
      super.unsetErasureCodingPolicy(path);
    }
  }

  /**
   * Get the root directory of Trash for a path in HDFS.
   * 1. File in encryption zone returns /ez1/.Trash/username
   * 2. File not in encryption zone, or encountered exception when checking
   *    the encryption zone of the path, returns /users/username/.Trash
   * Caller appends either Current or checkpoint timestamp for trash destination
   * @param path the trash root of the path to be determined.
   * @return trash root
   */
  @Override
  public Path getTrashRoot(Path path) {
    if (useUnderFs()) {
      return floatPath(underFs.getTrashRoot(divePath(path)));
    } else {
      return super.getTrashRoot(path);
    }
  }

  /**
   * Get all the trash roots of HDFS for current user or for all the users.
   * 1. File deleted from non-encryption zone /user/username/.Trash
   * 2. File deleted from encryption zones
   *    e.g., ez1 rooted at /ez1 has its trash root at /ez1/.Trash/$USER
   * @param allUsers return trashRoots of all users if true, used by emptier
   * @return trash roots of HDFS
   */
  @Override
  public Collection<FileStatus> getTrashRoots(boolean allUsers) {
    if (useUnderFs()) {
      Collection<FileStatus> orig = underFs.getTrashRoots(allUsers);
      List<FileStatus> res = new ArrayList<>();
      for (FileStatus fs : orig) {
        try {
          res.add(floatFileStatus(fs));
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      return res;
    } else {
      return super.getTrashRoots(allUsers);
    }
  }

  @Override
  protected Path fixRelativePart(Path p) {
    return super.fixRelativePart(p);
  }

  Statistics getFsStatistics() {
    return statistics;
  }

  DFSOpsCountStatistics getDFSOpsCountStatistics() {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri,
          "getDFSOpsCountStatistics()");
    } else {
      return super.getDFSOpsCountStatistics();
    }
  }

  /**
   * Create a HdfsDataOutputStreamBuilder to create a file on DFS.
   * Similar to {@link #create(Path)}, file is overwritten by default.
   *
   * @param path the path of the file to create.
   * @return A HdfsDataOutputStreamBuilder for creating a file.
   */
  @Override
  public DistributedFileSystem.HdfsDataOutputStreamBuilder createFile(Path path) {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri, "createFile(Path)");
    } else {
      return super.createFile(path);
    }
  }

  /**
   * Returns a RemoteIterator which can be used to list all open files
   * currently managed by the NameNode. For large numbers of open files,
   * iterator will fetch the list in batches of configured size.
   * <p>
   * Since the list is fetched in batches, it does not represent a
   * consistent snapshot of the all open files.
   * <p>
   * This method can only be called by HDFS superusers.
   */
  @Deprecated
  public RemoteIterator<OpenFileEntry> listOpenFiles() throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri, "listOpenFiles()");
    } else {
      return super.listOpenFiles();
    }
  }

  public RemoteIterator<OpenFileEntry> listOpenFiles(
      EnumSet<OpenFilesIterator.OpenFilesType> openFilesTypes, String path) throws IOException {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri,
          "listOpenFiles(EnumSet<OpenFilesType>, String)");
    } else {
      return super.listOpenFiles(openFilesTypes, path);
    }
  }


  /**
   * Create a {@link DistributedFileSystem.HdfsDataOutputStreamBuilder} to append a file on DFS.
   *
   * @param path file path.
   * @return A {@link DistributedFileSystem.HdfsDataOutputStreamBuilder} for appending a file.
   */
  @Override
  public DistributedFileSystem.HdfsDataOutputStreamBuilder appendFile(Path path) {
    if (useUnderFs()) {
      throw new UnderFSNotSupportedOperation(underUri, "appendFile(Path)");
    } else {
      return super.appendFile(path);
    }
  }
}
