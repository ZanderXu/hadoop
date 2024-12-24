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

package org.apache.hadoop.hdfs.server.namenode.fgl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.qjournal.MiniQJMHACluster;
import org.apache.hadoop.hdfs.util.RwLockMode;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * To test {@link FSNLockBenchmarkThroughput}.
 */
public class TestFSNLockBenchmarkThroughput {

  private static final Logger LOG = LoggerFactory.getLogger(TestFSNLockBenchmarkThroughput.class);

  @Test
  public void testFineGrainedLockingBenchmarkThroughput1() throws Exception {
    testBenchmarkThroughput(true, 20, 100, 1000);
  }

  @Test
  public void testFineGrainedLockingBenchmarkThroughput2() throws Exception {
    testBenchmarkThroughput(true, 20, 1000, 1000);
  }

  @Test
  public void testFineGrainedLockingBenchmarkThroughput3() throws Exception {
    testBenchmarkThroughput(true, 10, 100, 100);
  }

  @Test
  public void testGlobalLockingBenchmarkThroughput1() throws Exception {
    testBenchmarkThroughput(false, 20, 100, 1000);
  }

  @Test
  public void testGlobalLockingBenchmarkThroughput2() throws Exception {
    testBenchmarkThroughput(false, 20, 1000, 1000);
  }

  @Test
  public void testGlobalLockingBenchmarkThroughput3() throws Exception {
    testBenchmarkThroughput(false, 10, 100, 100);
  }

  @Test
  public void testReadLockBenchmarkThroughput() {
    FineGrainedFSNamesystemLock fineGrainedLock = new FineGrainedFSNamesystemLock(new Configuration(), null);
    GlobalFSNamesystemLock globalLock = new GlobalFSNamesystemLock(new Configuration(), null);
    int loopCount = 1000000;
    Map<String, ArrayList<Long>> results = new HashMap<>();
    results.put("FGL_FS", new ArrayList<>());
    results.put("FGL_BM", new ArrayList<>());
    results.put("FGL_GLOBAL", new ArrayList<>());
    results.put("GLOBAL_FS", new ArrayList<>());
    results.put("GLOBAL_BM", new ArrayList<>());
    results.put("GLOBAL_GLOBAL", new ArrayList<>());

    for (int i = 0; i < loopCount; i++) {
      fineGrainedLock.readLock(RwLockMode.FS);
      fineGrainedLock.readUnlock(RwLockMode.FS, "Test");
    }

    for (int i = 0; i < loopCount; i++) {
      globalLock.readLock(RwLockMode.FS);
      globalLock.readUnlock(RwLockMode.FS, "Test");
    }

    for (int j = 0; j < 10; j++) {
      long fglFSStart = System.nanoTime();
      for (int i = 0; i < loopCount; i++) {
        fineGrainedLock.readLock(RwLockMode.FS);
        fineGrainedLock.readUnlock(RwLockMode.FS, "Test");
      }
      long fglFSEnd = System.nanoTime();

      results.get("FGL_FS").add(fglFSEnd - fglFSStart);

      LOG.info("FGL FS Lock takes {}(ns)", (fglFSEnd - fglFSStart));

      long fglBMStart = System.nanoTime();
      for (int i = 0; i < loopCount; i++) {
        fineGrainedLock.readLock(RwLockMode.BM);
        fineGrainedLock.readUnlock(RwLockMode.BM, "Test");
      }
      long fglBMEnd = System.nanoTime();

      LOG.info("FGL BM Lock takes {}(ns)", (fglBMEnd - fglBMStart));
      results.get("FGL_BM").add(fglBMEnd - fglBMStart);

      long fglGlobalStart = System.nanoTime();
      for (int i = 0; i < loopCount; i++) {
        fineGrainedLock.readLock(RwLockMode.GLOBAL);
        fineGrainedLock.readUnlock(RwLockMode.GLOBAL, "Test");
      }
      long fglGlobalEnd = System.nanoTime();

      results.get("FGL_GLOBAL").add(fglGlobalEnd - fglGlobalStart);
      LOG.info("FGL GLOBAL Lock takes {}(ns)", (fglGlobalEnd - fglGlobalStart));

      long globalFSStart = System.nanoTime();
      for (int i = 0; i < loopCount; i++) {
        globalLock.readLock(RwLockMode.FS);
        globalLock.readUnlock(RwLockMode.FS, "Test");
      }
      long globalFSEnd = System.nanoTime();
      results.get("GLOBAL_FS").add(globalFSEnd - globalFSStart);
      LOG.info("GLOBAL FS Lock takes {}(ns)", (globalFSEnd - globalFSStart));

      long globalBMStart = System.nanoTime();
      for (int i = 0; i < loopCount; i++) {
        globalLock.readLock(RwLockMode.BM);
        globalLock.readUnlock(RwLockMode.BM, "Test");
      }
      long globalBMEnd = System.nanoTime();
      results.get("GLOBAL_BM").add(globalBMEnd - globalBMStart);
      LOG.info("GLOBAL BM Lock takes {}(ns)", (globalBMEnd - globalBMStart));

      long globalGlobalStart = System.nanoTime();
      for (int i = 0; i < loopCount; i++) {
        globalLock.readLock(RwLockMode.GLOBAL);
        globalLock.readUnlock(RwLockMode.GLOBAL, "Test");
      }
      long globalGlobalEnd = System.nanoTime();
      results.get("GLOBAL_GLOBAL").add(globalGlobalEnd - globalGlobalStart);
      LOG.info("GLOBAL GLOBAL Lock takes {}(ns)", (globalGlobalEnd - globalGlobalStart));
    }

    results.forEach((k, v) -> {
      long sumValue = v.stream().mapToLong(Long::longValue).sum();
      LOG.info("{} avg time is {}(ns)", k, sumValue/v.size());
    });

    LOG.info("Final Results: {}", results);
  }

  @Test
  public void testWriteLockBenchmarkThroughput() {
    FineGrainedFSNamesystemLock fineGrainedLock = new FineGrainedFSNamesystemLock(new Configuration(), null);
    GlobalFSNamesystemLock globalLock = new GlobalFSNamesystemLock(new Configuration(), null);
    int loopCount = 1000000;
    Map<String, ArrayList<Long>> results = new HashMap<>();
    results.put("FGL_FS", new ArrayList<>());
    results.put("FGL_BM", new ArrayList<>());
    results.put("FGL_GLOBAL", new ArrayList<>());
    results.put("GLOBAL_FS", new ArrayList<>());
    results.put("GLOBAL_BM", new ArrayList<>());
    results.put("GLOBAL_GLOBAL", new ArrayList<>());

    for (int i = 0; i < loopCount; i++) {
      fineGrainedLock.writeLock(RwLockMode.FS);
      fineGrainedLock.writeUnlock(RwLockMode.FS, "Test");
    }

    for (int i = 0; i < loopCount; i++) {
      globalLock.writeLock(RwLockMode.FS);
      globalLock.writeUnlock(RwLockMode.FS, "Test");
    }

    for (int j = 0; j < 10; j++) {
      long fglFSStart = System.nanoTime();
      for (int i = 0; i < loopCount; i++) {
        fineGrainedLock.writeLock(RwLockMode.FS);
        fineGrainedLock.writeUnlock(RwLockMode.FS, "Test");
      }
      long fglFSEnd = System.nanoTime();

      results.get("FGL_FS").add(fglFSEnd - fglFSStart);

      LOG.info("FGL FS Lock takes {}(ns)", (fglFSEnd - fglFSStart));

      long fglBMStart = System.nanoTime();
      for (int i = 0; i < loopCount; i++) {
        fineGrainedLock.writeLock(RwLockMode.BM);
        fineGrainedLock.writeUnlock(RwLockMode.BM, "Test");
      }
      long fglBMEnd = System.nanoTime();

      LOG.info("FGL BM Lock takes {}(ns)", (fglBMEnd - fglBMStart));
      results.get("FGL_BM").add(fglBMEnd - fglBMStart);

      long fglGlobalStart = System.nanoTime();
      for (int i = 0; i < loopCount; i++) {
        fineGrainedLock.writeLock(RwLockMode.GLOBAL);
        fineGrainedLock.writeUnlock(RwLockMode.GLOBAL, "Test");
      }
      long fglGlobalEnd = System.nanoTime();

      results.get("FGL_GLOBAL").add(fglGlobalEnd - fglGlobalStart);
      LOG.info("FGL GLOBAL Lock takes {}(ns)", (fglGlobalEnd - fglGlobalStart));

      long globalFSStart = System.nanoTime();
      for (int i = 0; i < loopCount; i++) {
        globalLock.writeLock(RwLockMode.FS);
        globalLock.writeUnlock(RwLockMode.FS, "Test");
      }
      long globalFSEnd = System.nanoTime();
      results.get("GLOBAL_FS").add(globalFSEnd - globalFSStart);
      LOG.info("GLOBAL FS Lock takes {}(ns)", (globalFSEnd - globalFSStart));

      long globalBMStart = System.nanoTime();
      for (int i = 0; i < loopCount; i++) {
        globalLock.writeLock(RwLockMode.BM);
        globalLock.writeUnlock(RwLockMode.BM, "Test");
      }
      long globalBMEnd = System.nanoTime();
      results.get("GLOBAL_BM").add(globalBMEnd - globalBMStart);
      LOG.info("GLOBAL BM Lock takes {}(ns)", (globalBMEnd - globalBMStart));

      long globalGlobalStart = System.nanoTime();
      for (int i = 0; i < loopCount; i++) {
        globalLock.writeLock(RwLockMode.GLOBAL);
        globalLock.writeUnlock(RwLockMode.GLOBAL, "Test");
      }
      long globalGlobalEnd = System.nanoTime();
      results.get("GLOBAL_GLOBAL").add(globalGlobalEnd - globalGlobalStart);
      LOG.info("GLOBAL GLOBAL Lock takes {}(ns)", (globalGlobalEnd - globalGlobalStart));
    }

    results.forEach((k, v) -> {
      long sumValue = v.stream().mapToLong(Long::longValue).sum();
      LOG.info("{} avg time is {}(ns)", k, sumValue/v.size());
    });

    LOG.info("Final Results: {}", results);
  }

  @Test
  public void testPerRPC() throws Exception {
    int testingCount = 100000;
    int numClients = 100;
    testPerRPCInternal(true, testingCount, numClients, "create");
    testPerRPCInternal(false, testingCount, numClients, "create");

   testPerRPCInternal(true, testingCount, numClients, "getFileInfo");
   testPerRPCInternal(false, testingCount, numClients, "getFileInfo");

    testPerRPCInternal(true, testingCount, numClients, "getBlockLocation");
    testPerRPCInternal(false, testingCount, numClients, "getBlockLocation");

    testPerRPCInternal(true, testingCount, numClients, "heartbeat");
    testPerRPCInternal(false, testingCount, numClients, "heartbeat");

    testPerRPCInternal(true, testingCount, numClients, "ibr");
    testPerRPCInternal(false, testingCount, numClients, "ibr");
  }

  private void testPerRPCInternal(boolean enableFGL,
      int testingCount, int numClients, String testType) throws Exception {
    MiniQJMHACluster qjmhaCluster = null;
    int loopCount = 5;
    try {
      Configuration conf = new HdfsConfiguration();
      conf.setBoolean(DFSConfigKeys.DFS_HA_TAILEDITS_INPROGRESS_KEY, true);
      conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_FSLOCK_FAIR_KEY, false);
      conf.setInt(DFSConfigKeys.DFS_QJOURNAL_SELECT_INPUT_STREAMS_TIMEOUT_KEY, 500);
      if (enableFGL) {
        conf.setClass(DFSConfigKeys.DFS_NAMENODE_LOCK_MODEL_PROVIDER_KEY,
            FineGrainedFSNamesystemLock.class, FSNLockManager.class);
      } else {
        conf.setClass(DFSConfigKeys.DFS_NAMENODE_LOCK_MODEL_PROVIDER_KEY,
            GlobalFSNamesystemLock.class, FSNLockManager.class);
      }

      MiniQJMHACluster.Builder builder = new MiniQJMHACluster.Builder(conf);
      builder.getDfsBuilder().numDataNodes(10);
      qjmhaCluster = builder.build();
      MiniDFSCluster cluster = qjmhaCluster.getDfsCluster();

      cluster.transitionToActive(0);
      cluster.waitActive(0);

      FileSystem fileSystem = cluster.getFileSystem(0);
      FSNLockBenchmarkThroughput fsnLockBenchmarkThroughput =
          new FSNLockBenchmarkThroughput(fileSystem);

      Path basePath = new Path("/tmp/fsnlock/benchmark/throughput");
      // private final
      ArrayList<Path> readingPaths = fsnLockBenchmarkThroughput.createReadingFiles(basePath);
      for (int i = 0; i < loopCount; i++) {
        if (testType.equals("create")) {
          fsnLockBenchmarkThroughput.benchmarkForCreate(basePath, testingCount, numClients);
        } else if (testType.equals("getFileInfo")) {
          fsnLockBenchmarkThroughput.setCluster(cluster);
          fsnLockBenchmarkThroughput.benchmarkForGetFileInfo(readingPaths, testingCount, numClients);
        } else if (testType.equals("getBlockLocation")) {
          fsnLockBenchmarkThroughput.setCluster(cluster);
          fsnLockBenchmarkThroughput.benchmarkForGetBlockLocation(readingPaths, testingCount, numClients);
        } else if (testType.equals("heartbeat")) {
          fsnLockBenchmarkThroughput.setCluster(cluster);
          fsnLockBenchmarkThroughput.benchmarkForHeartbeat(testingCount, numClients);
        } else if (testType.equals("ibr")) {
          fsnLockBenchmarkThroughput.setCluster(cluster);
          fsnLockBenchmarkThroughput.benchmarkForIBR(testingCount, numClients);
        }
      }
      fsnLockBenchmarkThroughput.deleteReadingFiles(readingPaths);
    } finally {
      if (qjmhaCluster != null) {
        qjmhaCluster.shutdown();
      }
    }
  }

  private void testBenchmarkThroughput(boolean enableFGL, int readWriteRatio,
      int testingCount, int numClients) throws Exception {
    MiniQJMHACluster qjmhaCluster = null;

    try {
      Configuration conf = new HdfsConfiguration();
      conf.setBoolean(DFSConfigKeys.DFS_HA_TAILEDITS_INPROGRESS_KEY, true);
      conf.setInt(DFSConfigKeys.DFS_QJOURNAL_SELECT_INPUT_STREAMS_TIMEOUT_KEY, 500);
      if (enableFGL) {
        conf.setClass(DFSConfigKeys.DFS_NAMENODE_LOCK_MODEL_PROVIDER_KEY,
            FineGrainedFSNamesystemLock.class, FSNLockManager.class);
      } else {
        conf.setClass(DFSConfigKeys.DFS_NAMENODE_LOCK_MODEL_PROVIDER_KEY,
            GlobalFSNamesystemLock.class, FSNLockManager.class);
      }

      MiniQJMHACluster.Builder builder = new MiniQJMHACluster.Builder(conf);
      builder.getDfsBuilder().numDataNodes(10);
      qjmhaCluster = builder.build();
      MiniDFSCluster cluster = qjmhaCluster.getDfsCluster();

      cluster.transitionToActive(0);
      cluster.waitActive(0);

      FileSystem fileSystem = cluster.getFileSystem(0);

      String[] args = new String[]{"/tmp/fsnlock/benchmark/throughput",
          String.valueOf(readWriteRatio), String.valueOf(testingCount),
          String.valueOf(numClients)};

      Assert.assertEquals(0, ToolRunner.run(conf,
          new FSNLockBenchmarkThroughput(fileSystem), args));
    } finally {
      if (qjmhaCluster != null) {
        qjmhaCluster.shutdown();
      }
    }
  }
}
