package org.apache.hadoop.hdfs.server.namenode.ha;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.qjournal.MiniJournalCluster;
import org.apache.hadoop.hdfs.qjournal.MiniQJMHACluster;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos;
import org.apache.hadoop.hdfs.qjournal.server.JournalNode;
import org.apache.hadoop.hdfs.qjournal.server.JournalNodeRpcServer;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_JOURNALNODE_ENABLE_SYNC_KEY;
import static org.apache.hadoop.hdfs.qjournal.client.QuorumJournalManager.QJM_RPC_MAX_TXNS_KEY;
import static org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter.getFileInfo;
import static org.mockito.ArgumentMatchers.eq;

public class TestStandbyInProgressTailWithHA {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestStandbyInProgressTailWithHA.class);
  private Configuration conf;
  private MiniQJMHACluster qjmhaCluster;
  private MiniDFSCluster cluster;

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
    /*conf.setInt(QJM_RPC_MAX_TXNS_KEY, 3);*/
    HAUtil.setAllowStandbyReads(conf, true);

    // start 3 journal nodes
    MiniJournalCluster.Builder builder = new MiniJournalCluster.Builder(conf)
        .format(true);

    File baseDir = new File(MiniDFSCluster.getBaseDirectory());
    JournalNode[] journalNodes = new JournalNode[builder.getNumJournalNodes()];
    for (int i = 0; i < builder.getNumJournalNodes(); i++) {
      File dir = new File(baseDir, "journalnode-" + i).getAbsoluteFile();;
      FileUtil.fullyDelete(dir);
      JournalNode jn = Mockito.spy(JournalNode.class);
      jn.setConf(createConfForNode(conf, dir));
      JournalNodeRpcServer
          mockedJNRpcServer = Mockito.spy(new JournalNodeRpcServer(jn.getConf(), jn));
      if (i == 0) {
        QJournalProtocolProtos.GetJournaledEditsResponseProto responseProto = QJournalProtocolProtos.GetJournaledEditsResponseProto
            .newBuilder().setTxnCount(0).build();
        Mockito.doReturn(responseProto).when(mockedJNRpcServer)
            .getJournaledEdits(eq("ns1"), eq("ns1"),
                eq(1L), eq(5000));
      } else if (i == 2) {
        QJournalProtocolProtos.GetJournaledEditsResponseProto responseFromJN2 =
            journalNodes[1].getRpcServer().
                getJournaledEdits("ns1", "ns1", 1, 5000);
        Mockito.doAnswer(invocation -> {
          System.out.println("0000000 sleep");
          Thread.sleep(100);
          return responseFromJN2;
        }).when(mockedJNRpcServer).getJournaledEdits(
            "ns1", "ns1", 1L, 5000);
      }
      jn.startWithMockRpcServer(mockedJNRpcServer);
      journalNodes[i] = jn;
    }
    builder.journalNodes(journalNodes);
    qjmhaCluster = new MiniQJMHACluster.Builder(conf).setMiniJournalCluster(Mockito.spy(builder.build())).build();
    cluster = qjmhaCluster.getDfsCluster();

    // Get NameNode from cluster to future manual control
    nn0 = cluster.getNameNode(0);
    nn1 = cluster.getNameNode(1);
  }

  private Configuration createConfForNode(Configuration conf, File logDir) {
    Configuration copyConf = new Configuration(conf);
    copyConf.set(DFSConfigKeys.DFS_JOURNALNODE_EDITS_DIR_KEY, logDir.toString());
    copyConf.set(DFSConfigKeys.DFS_JOURNALNODE_RPC_ADDRESS_KEY, "localhost:0");
    copyConf.set(DFSConfigKeys.DFS_JOURNALNODE_HTTP_ADDRESS_KEY, "localhost:0");
    return copyConf;
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
    QJournalProtocolProtos.GetJournaledEditsResponseProto responseProto =
        qjmhaCluster.getJournalCluster().getJournalNode(0).getRpcServer().getJournaledEdits("ns1", "ns1", 1, 5000);
    System.out.println("3333333 - 0 " + responseProto);

    System.out.println("1111111 ");
    cluster.transitionToActive(1);
    nn1.getNamesystem().startActiveServices();
    responseProto =
        qjmhaCluster.getJournalCluster().getJournalNode(0).getRpcServer().getJournaledEdits("ns1", "ns1", 1, 5000);
    System.out.println("3333333 - 1" + responseProto);
    cluster.waitActive(1);
    responseProto =
        qjmhaCluster.getJournalCluster().getJournalNode(0).getRpcServer().getJournaledEdits("ns1", "ns1", 1, 5000);
    System.out.println("3333333 - 2 " + responseProto);


    waitForFileInfo(nn1, p + 0, p + 1, p + 14);
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
  private static void waitForFileInfo(NameNode standbyNN, String... fileNames)
      throws Exception {
    List<String> remainingFiles = Lists.newArrayList(fileNames);
    GenericTestUtils.waitFor(() -> {
      try {
        standbyNN.getNamesystem().getEditLogTailer().doTailEdits();
        for (Iterator<String> it = remainingFiles.iterator(); it.hasNext();) {
          if (getFileInfo(standbyNN, it.next(), true, false, false) == null) {
            return false;
          } else {
            it.remove();
          }
        }
        return true;
      } catch (IOException|InterruptedException e) {
        throw new AssertionError("Exception while waiting: " + e);
      }
    }, 10, 1000);
  }
}
