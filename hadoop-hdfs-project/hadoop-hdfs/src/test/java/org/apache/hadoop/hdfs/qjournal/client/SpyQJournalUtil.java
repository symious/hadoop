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
package org.apache.hadoop.hdfs.qjournal.client;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetJournaledEditsResponseProto;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.eq;

/**
 * One Util class to mock QJuournals for some UTs not in this package.
 */
public class SpyQJournalUtil {

  /**
   * Mock a QuorumJournalManager with input uri, nsInfo and namServiceId.
   * @param conf input configuration.
   * @param uri input uri.
   * @param nsInfo input nameservice info.
   * @return one mocked QuorumJournalManager.
   * @throws IOException throw IOException.
   */
  public static QuorumJournalManager createSpyingQJM(Configuration conf,
      URI uri, NamespaceInfo nsInfo) throws IOException {
    AsyncLogger.Factory spyFactory = new AsyncLogger.Factory() {
      @Override
      public AsyncLogger createLogger(Configuration conf1, NamespaceInfo nsInfo1,
          String journalId1, InetSocketAddress addr1) {
        AsyncLogger logger = new IPCLoggerChannel(conf1, nsInfo1, journalId1, addr1);
        return Mockito.spy(logger);
      }
    };
    return new QuorumJournalManager(conf, uri, nsInfo, spyFactory);
  }

  /**
   * Try to mock one abnormal JournalNode with one empty response
   * for getJournaledEdits rpc with startTxid.
   * @param manager QuorumJournalmanager.
   * @param startTxid input StartTxid.
   */
  public static void mockOneJNReturnEmptyResponse(
      QuorumJournalManager manager, long startTxid, int journalIndex) {
    List<AsyncLogger> spies = manager.getLoggerSetForTests().getLoggersForTests();

    // Mock JN0 return an empty response.
    GetJournaledEditsResponseProto responseProto = GetJournaledEditsResponseProto
        .newBuilder().setTxnCount(journalIndex).build();
    ListenableFuture<GetJournaledEditsResponseProto> ret = Futures.immediateFuture(responseProto);
    Mockito.doReturn(ret).when(spies.get(journalIndex))
        .getJournaledEdits(eq(startTxid), eq(QuorumJournalManager.QJM_RPC_MAX_TXNS_DEFAULT));
  }

  /**
   * Try to mock one abnormal JournalNode with slow response for
   * getJournaledEdits rpc with startTxid.
   * @param manager input QuormJournalManager.
   * @param startTxid input start txid.
   * @param sleepTime sleep time.
   * @param journalIndex the journal index need to be mocked.
   */
  public static void mockOneJNWithSlowResponse(final QuorumJournalManager manager,
      final long startTxid, final int sleepTime, final int journalIndex) {
    List<AsyncLogger> spies = manager.getLoggerSetForTests().getLoggersForTests();

    final ListeningExecutorService service = MoreExecutors.listeningDecorator(
        Executors.newSingleThreadExecutor());
    Mockito.doAnswer(new Answer<Object>() {
      @Override
      public Object answer(final InvocationOnMock invocation) {
        return service.submit(new Callable<Object>() {
          @Override
          public Object call() throws InterruptedException, ExecutionException {
            Thread.sleep(sleepTime);
            ListenableFuture<?> future = null;
            try {
              future = (ListenableFuture<?>) invocation.callRealMethod();
            } catch (Throwable e) {
              fail("getJournaledEdits failed " + e.getMessage());
            }
            return future.get();
          }
        });
      }
    }).when(spies.get(journalIndex))
        .getJournaledEdits(startTxid, QuorumJournalManager.QJM_RPC_MAX_TXNS_DEFAULT);
  }
}
