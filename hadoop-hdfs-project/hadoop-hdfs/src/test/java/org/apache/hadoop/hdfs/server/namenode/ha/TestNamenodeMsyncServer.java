/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode.ha;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.ClientGSIContext;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.protocol.ClientMsyncProtocol;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.namenode.NameNodeRpcServer;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_MSYNC_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_STATE_CONTEXT_ENABLED_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestNamenodeMsyncServer {
  @Rule
  public Timeout timeout = new Timeout(60000);

  private MiniDFSCluster cluster;
  private HdfsConfiguration conf;

  @Before
  public void setup() throws Exception {
    // Configure cluster with lifeline RPC server enabled, and down-tune
    // heartbeat timings to try to force quick dead/stale DataNodes.
    conf = new HdfsConfiguration();
    conf.setBoolean(DFS_NAMENODE_STATE_CONTEXT_ENABLED_KEY, true);
    conf.set(DFS_NAMENODE_MSYNC_RPC_ADDRESS_KEY, "0.0.0.0:0");

    cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleHATopology())
        .numDataNodes(3)
        .build();

    cluster.transitionToActive(0);
    cluster.waitActive();

    RPC.Server msyncServer = ((NameNodeRpcServer)cluster.getNameNodeRpc(0))
        .getMsyncRpcServer();
    assertNotNull(msyncServer);
  }

  @After
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testMSync2RPC() throws IOException {
    DistributedFileSystem dfs = cluster.getFileSystem(0);
    Path testPath = new Path("/test_msync");
    FSDataOutputStream out = dfs.create(testPath);
    out.write("Hello word".getBytes());
    out.close();

    long lastTxid = cluster.getNamesystem(0).getFSImage().getLastAppliedOrWrittenTxId();
    assertTrue(lastTxid > 0);

    HAProxyFactory<ClientProtocol> clientProtocolHAProxyFactory = new ClientHAProxyFactory<>();
    ClientGSIContext clientGSIContext = new ClientGSIContext();
    clientProtocolHAProxyFactory.setAlignmentContext(clientGSIContext);

    ClientProtocol clientProtocol = clientProtocolHAProxyFactory.createProxy(conf, cluster.getNameNode(0)
            .getNameNodeAddress(), ClientProtocol.class, UserGroupInformation.getCurrentUser(),
        false, new AtomicBoolean(false));
    clientProtocol.msync();
    long clientLastSeenStateId = clientGSIContext.getLastSeenStateId();

    assertEquals(lastTxid, clientLastSeenStateId);

    HAProxyFactory<ClientMsyncProtocol> clientMSyncProtocolHAProxyFactory = new ClientHAProxyFactory<>();
    ClientGSIContext clientMSyncGSIContext = new ClientGSIContext();
    clientMSyncProtocolHAProxyFactory.setAlignmentContext(clientMSyncGSIContext);

    ClientMsyncProtocol clientMSyncProtocol = clientMSyncProtocolHAProxyFactory.createProxy(
        conf, cluster.getNameNode(0).getNameNodeMSyncAddress(),
        ClientMsyncProtocol.class, UserGroupInformation.getCurrentUser(),
        false, new AtomicBoolean(false));
    clientMSyncProtocol.msync();

    long clientMSycn2LastSeenStateId = clientMSyncGSIContext.getLastSeenStateId();

    assertEquals(lastTxid, clientMSycn2LastSeenStateId);
  }
}
