package org.apache.hadoop.hdfs.server.namenode;

import com.jcraft.jsch.IO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.qjournal.MiniJournalCluster;
import org.apache.hadoop.hdfs.qjournal.MiniQJMHACluster;
import org.apache.hadoop.hdfs.qjournal.client.AsyncLogger;
import org.apache.hadoop.hdfs.qjournal.client.DirectExecutorService;
import org.apache.hadoop.hdfs.qjournal.client.IPCLoggerChannel;
import org.apache.hadoop.hdfs.qjournal.client.QuorumJournalManager;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerFaultInjector;
import org.apache.hadoop.hdfs.server.namenode.ha.EditLogTailer;
import org.apache.hadoop.hdfs.server.namenode.ha.TestStandbyInProgressTail;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.Futures;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.MoreExecutors;
import org.apache.hadoop.util.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_JOURNALNODE_ENABLE_SYNC_KEY;
import static org.apache.hadoop.hdfs.qjournal.QJMTestUtil.FAKE_NSINFO;
import static org.apache.hadoop.hdfs.qjournal.QJMTestUtil.JID;
import static org.apache.hadoop.hdfs.qjournal.client.QuorumJournalManager.QJM_RPC_MAX_TXNS_KEY;
import static org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter.getFileInfo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class TestEditLogJournalFailures1 {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestStandbyInProgressTail.class);
  private Configuration conf;
  private MiniQJMHACluster qjmhaCluster;
  private MiniDFSCluster cluster;
  private MiniJournalCluster jnCluster;
  private NameNode nn0;
  private NameNode nn1;

  @Before
  public void startUp() throws IOException {
    conf = new Configuration();
    // Set period of tail edits to a large value (20 mins) for test purposes
    conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, 20 * 60);
    conf.setBoolean(DFSConfigKeys.DFS_HA_TAILEDITS_INPROGRESS_KEY, true);
    conf.setInt(DFSConfigKeys.DFS_QJOURNAL_SELECT_INPUT_STREAMS_TIMEOUT_KEY,
        500);
    conf.setBoolean(DFS_JOURNALNODE_ENABLE_SYNC_KEY, false);
    // Set very samll limit of transactions per a journal rpc call
    // conf.setInt(QJM_RPC_MAX_TXNS_KEY, 3);
    HAUtil.setAllowStandbyReads(conf, true);
    qjmhaCluster = new MiniQJMHACluster.Builder(conf).setMockJN(true).build();
    cluster = qjmhaCluster.getDfsCluster();
    jnCluster = qjmhaCluster.getJournalCluster();

    // Get NameNode from cluster to future manual control
    nn0 = cluster.getNameNode(0);
    nn1 = cluster.getNameNode(1);
  }

  @After
  public void tearDown() throws IOException {
    if (qjmhaCluster != null) {
      qjmhaCluster.shutdown();
    }
  }


  /**
   * Test that Standby Node tails multiple segments while catching up
   * during the transition to Active.
   */
  @Test
  public void testFailoverWithAbnormalJN() throws Exception {
    cluster.transitionToActive(0);
    cluster.waitActive(0);

    cluster.getNameNode(1).getNamesystem().getEditLogTailer().stop();

    System.out.println("123456789 ");
    String p = "/testFailoverWhileTailingWithoutCache/";
    mkdirs(nn0, p + 0, p + 1, p + 2, p + 3, p + 4);
    mkdirs(nn0, p + 5, p + 6, p + 7, p + 8, p + 9);
    mkdirs(nn0, p + 10, p + 11, p + 12, p + 13, p + 14);


    cluster.transitionToStandby(0);
    nn0.stop();


    BlockManagerFaultInjector.instance = new BlockManagerFaultInjector() {
      @Override
      public void mockJNStreams() throws IOException {
        spyOnJASjournal();
      }
    };;

    nn1.getRpcServer().transitionToActive(
        new HAServiceProtocol.StateChangeRequestInfo(
            HAServiceProtocol.RequestSource.REQUEST_BY_USER_FORCED));

    System.out.println("==========begin=========");
    // spyOnJASjournal();

    System.out.println("==========end=========");
    // nn1.getNamesystem().startActiveServices();

    System.out.println("==========end - 2 =========");

    waitForFileInfo(nn1, p + 0, p + 1, p + 14);
  }

  private QuorumJournalManager createSpyingQJM(Configuration conf)
      throws IOException {
    AsyncLogger.Factory spyFactory =
        (conf1, nsInfo, journalId, nameServiceId, addr) -> {
          AsyncLogger logger = new IPCLoggerChannel(conf1, nsInfo, journalId,
              nameServiceId, addr) {
            protected ExecutorService createSingleThreadExecutor() {
              // Don't parallelize calls to the quorum in the tests.
              // This makes the tests more deterministic.
              return new DirectExecutorService();
            }
          };

          return Mockito.spy(logger);
        };
    return new QuorumJournalManager(
        conf, jnCluster.getQuorumJournalURI("ns1"),  nn1.getNamesystem().getNamespaceInfo(),
        "ns1", spyFactory);
  }

  /**
   * Pull out one of the JournalAndStream objects from the edit log.
   */
  private JournalSet.JournalAndStream getJournalAndStream(int index) {
    FSEditLog editLog = nn1.getFSImage().getEditLog();
    return editLog.getJournals().get(index);
  }


  private void spyOnJASjournal() throws IOException {
    EditLogTailer tailer = nn1.getNamesystem().getEditLogTailer();
    FSEditLog editLog = tailer.getEditLog();
    JournalSet journalSet = editLog.getJournalSet();
    JournalSet.JournalAndStream jas = journalSet.getAllJournalStreams().get(0);

    JournalManager oldManager = jas.getManager();

    System.out.println("111111 " + (oldManager instanceof QuorumJournalManager)
        + " editLog hashCode is " + editLog.hashCode() + ", jas hashCode is " + jas + ", managerHashCode is " + oldManager.hashCode() + ", tailer hashCode=" + tailer.hashCode() + ", setHashCode is " + journalSet.hashCode());
    oldManager.close();
    for (JournalSet.JournalAndStream a : journalSet.getAllJournalStreams()) {
      System.out.println("11111 a " + a + ", hashCode = " + a.hashCode());
    }


    QuorumJournalManager manager = createSpyingQJM(nn1.getConf());
    manager.recoverUnfinalizedSegments();
    jas.setJournalForTests(manager);
    System.out.println("111111  editLog hashCode is " + editLog.hashCode() + ", jas hashCode is " + jas + ", managerHashCode is " + manager.hashCode());
    for (JournalSet.JournalAndStream a : journalSet.getAllJournalStreams()) {
      System.out.println("11111 a " + a + ", hashCode = " + a.hashCode());
    }
    List<AsyncLogger> spies = manager.getLoggerSetForTests().getLoggersForTests();

    QJournalProtocolProtos.GetJournaledEditsResponseProto responseProto =
        QJournalProtocolProtos.GetJournaledEditsResponseProto
            .newBuilder().setTxnCount(0).build();

    ListenableFuture<QJournalProtocolProtos.GetJournaledEditsResponseProto> ret = Futures.immediateFuture(responseProto);
    Mockito.doReturn(ret).when(spies.get(0)).getJournaledEdits(eq(1L),
        eq(QuorumJournalManager.QJM_RPC_MAX_TXNS_DEFAULT));


    ListeningExecutorService service = MoreExecutors.listeningDecorator(
        Executors.newSingleThreadExecutor());
    Mockito.doAnswer(invocation -> service.submit(
        () -> {
          LOG.info("777777777777");
          Thread.sleep(3000);
          LOG.info("777777777777  -   1");
          ListenableFuture<QJournalProtocolProtos.GetJournaledEditsResponseProto> future = null;
          try {
            future = (ListenableFuture<QJournalProtocolProtos.GetJournaledEditsResponseProto>)invocation.callRealMethod();
          } catch (Throwable e) {
            e.printStackTrace();
          }
          return future.get();
        })
    ).when(spies.get(1)).getJournaledEdits(1,
        QuorumJournalManager.QJM_RPC_MAX_TXNS_DEFAULT);
  }

  /**
   * Create the given directories on the provided NameNode.
   */
  private static void mkdirs(NameNode nameNode, String... dirNames)
      throws Exception {
    for (String dirName : dirNames) {
      nameNode.getRpcServer().mkdirs(dirName,
          FsPermission.createImmutable((short) 0755), true);
    }
  }

  /**
   * Wait up to 1 second until the given NameNode is aware of the existing of
   * all of the provided fileNames.
   */
  private static void waitForFileInfo(NameNode nn, String... fileNames)
      throws Exception {
    for (String fileName : fileNames){
      assertNotNull(getFileInfo(nn, fileName, true, false, false));
    }
  }
}
