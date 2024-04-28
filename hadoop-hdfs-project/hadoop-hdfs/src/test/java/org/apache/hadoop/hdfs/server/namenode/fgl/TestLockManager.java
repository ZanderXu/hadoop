package org.apache.hadoop.hdfs.server.namenode.fgl;

import com.sun.tools.javac.util.Pair;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.Time;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

public class TestLockManager {

  private final Logger LOG = LoggerFactory.getLogger(TestLockManager.class);

  private ArrayList<LockManager<String>> lockManagers;

  @Before
  public void getLockManagers() {
    ArrayList<LockManager<String>> lockManagers = new ArrayList<>();
    lockManagers.add(new CacheAllLockManager<>(10000, true, false));
    lockManagers.add(new MultiHashMapLockManager<>(10000, true, false));
    lockManagers.add(new ConcurrentHashMapLockManager<>(10000, true, false));
    lockManagers.add(new LockKeyLevelLockManager<>(10000, true, false));

    this.lockManagers = lockManagers;
  }

  @After
  public void closeLockManager() {
    for (LockManager<String> lockManager : this.lockManagers) {
      lockManager.close();
    }
  }

  @Test
  public void testAcquiringReadLock() throws Exception {
    for (LockManager<String> lockManager : this.lockManagers) {
      testAcquireReadLock(lockManager);
    }
  }

  private void testAcquireReadLock(LockManager<String> lockManager) throws Exception {
    String key1 = "mockKey1";
    String key2 = "mockKey2";

    // LockKey should not be null.
    LambdaTestUtils.intercept(IllegalArgumentException.class,
        () -> lockManager.acquireReadLock(null));

    AutoCloseNNLock<String> lock1 = lockManager.acquireReadLock(key1);
    Assert.assertEquals(1, lockManager.getNumberOfLocks());

    // Acquire a reentrant read lock for key1.
    AutoCloseNNLock<String> lock2 = lockManager.acquireReadLock(key1);
    Assert.assertEquals(1, lockManager.getNumberOfLocks());

    AutoCloseNNLock<String> lock3 = lockManager.acquireReadLock(key2);
    Assert.assertEquals(2, lockManager.getNumberOfLocks());

    // The current thread can not acquire the write lock while holding the read lock.
    // It may result in deadlock.
    LambdaTestUtils.intercept(IllegalStateException.class,
        () -> lockManager.acquireWriteLock(key1));

    lock1.close();
    lock2.close();
    lock3.close();

    if (!(lockManager instanceof CacheAllLockManager)) {;
      Assert.assertEquals(0, lockManager.getNumberOfLocks());
    }

    LambdaTestUtils.intercept(IllegalMonitorStateException.class, () -> lock3.close());

    AutoCloseNNLock<String> lock4 = lockManager.acquireReadLock(key1);
    if (!(lockManager instanceof CacheAllLockManager)) {
      Assert.assertEquals(1, lockManager.getNumberOfLocks());
    }
    lock4.close();
  }

  @Test
  public void testAcquiringWriteLock() throws Exception {
    for (LockManager<String> lockManager : this.lockManagers) {
      testAcquireWriteLock(lockManager);
    }
  }

  private void testAcquireWriteLock(LockManager<String> lockManager) throws Exception {
    String key1 = "mockKey1";
    String key2 = "mockKey2";

    LambdaTestUtils.intercept(IllegalArgumentException.class,
        () -> lockManager.acquireWriteLock(null));

    AutoCloseNNLock<String> lock1 = lockManager.acquireWriteLock(key1);
    Assert.assertEquals(1, lockManager.getNumberOfLocks());

    // Thread can acquire the read lock successfully while holding the write lock.
    AutoCloseNNLock<String> lock2 = lockManager.acquireReadLock(key1);
    Assert.assertEquals(1, lockManager.getNumberOfLocks());

    AutoCloseNNLock<String> lock3 = lockManager.acquireWriteLock(key2);
    Assert.assertEquals(2, lockManager.getNumberOfLocks());

    lock1.close();
    lock2.close();
    lock3.close();
    if (!(lockManager instanceof CacheAllLockManager)) {
      Assert.assertEquals(0, lockManager.getNumberOfLocks());
    }

    LambdaTestUtils.intercept(IllegalMonitorStateException.class, () -> lock3.close());
  }

  @Test
  public void testCloseClosedLock() throws Exception {
    for (LockManager<String> lockManager : this.lockManagers) {
      testCloseClosedLock(lockManager);
    }
  }

  private void testCloseClosedLock(LockManager<String> lockManager) throws Exception {
    String key1 = "mockKey1";
    AutoCloseNNLock<String> lock = lockManager.acquireReadLock(key1);
    lock.close();

    LambdaTestUtils.intercept(IllegalMonitorStateException.class,
        () -> lock.close());
  }

  @Test
  public void testAcquireReadLockWithHoldingWriteLock() {
    for (LockManager<String> lockManager : this.lockManagers) {
      testAcquireReadLockWithHoldingWriteLock(lockManager);
    }
  }

  private void testAcquireReadLockWithHoldingWriteLock(LockManager<String> lockManager) {
    String key1 = "mockKey1";
    LockInstance<String> lockInstance = lockManager.getLockInstanceFromPool(key1);

    AutoCloseNNLock<String> writeLock = lockInstance.getWriteLock();

    writeLock.lock();
    Assert.assertTrue(lockInstance.isWriteLockHeldByCurrentThread());

    AutoCloseNNLock<String> readLock = lockInstance.getReadLock();
    readLock.lock();
    Assert.assertTrue(lockInstance.isReadLockHeldByCurrentThread());
    Assert.assertTrue(lockInstance.isWriteLockHeldByCurrentThread());

    writeLock.close(false);
    Assert.assertTrue(lockInstance.isReadLockHeldByCurrentThread());
    Assert.assertFalse(lockInstance.isWriteLockHeldByCurrentThread());
  }

  @Test
  public void testChangingLock() throws Exception {
    for (LockManager<String> lockManager : this.lockManagers) {
      testChangeLock(lockManager);
      testChangeClosedLock(lockManager);
      testChangeReadLock2ReadLock(lockManager);
      testChangeWriteLock2WriteLock(lockManager);
    }
  }

  private void testChangeLock(LockManager<String> lockManager) {
    String key1 = "mockKey1";

    AutoCloseNNLock<String> lock = lockManager.acquireReadLock(key1);
    Assert.assertEquals(1, lockManager.getNumberOfLocks());

    // It will release the read lock then try to acquire the write lock again.
    lock = lockManager.changeReadLockToWriteLock(lock);
    Assert.assertEquals(1, lockManager.getNumberOfLocks());

    // It will acquire the read lock directly then releasing the write lock.
    lock = lockManager.changeWriteLockToReadLock(lock);
    Assert.assertEquals(1, lockManager.getNumberOfLocks());

    lock.close();
  }

  private void testChangeClosedLock(LockManager<String> lockManager) throws Exception {
    String key1 = "mockKey1";
    AutoCloseNNLock<String> lock1 = lockManager.acquireReadLock(key1);
    lock1.close();

    // The lock has been released from LockManager.
    LambdaTestUtils.intercept(IllegalMonitorStateException.class,
        () -> lockManager.changeWriteLockToReadLock(lock1));
    LambdaTestUtils.intercept(IllegalMonitorStateException.class,
        () -> lockManager.changeReadLockToWriteLock(lock1));
  }

  private void testChangeReadLock2ReadLock(LockManager<String> lockManager) throws Exception {
    String key1 = "mockKey1";
    try (AutoCloseNNLock<String> lock1 = lockManager.acquireReadLock(key1)) {
      LambdaTestUtils.intercept(IllegalMonitorStateException.class,
          () -> lockManager.changeWriteLockToReadLock(lock1));
    }
  }

  private void testChangeWriteLock2WriteLock(LockManager<String> lockManager) throws Exception {
    String key1 = "mockKey1";
    try (AutoCloseNNLock<String> lock1 = lockManager.acquireWriteLock(key1)) {
      LambdaTestUtils.intercept(IllegalMonitorStateException.class,
          () -> lockManager.changeReadLockToWriteLock(lock1));
    }
  }

  @Test
  @Timeout(300000)
  public void testLockManagersWithMultipleThreads() throws Exception {
    for (LockManager<String> lockManager : this.lockManagers) {
      LOG.info("TestLockManagersWithMultipleThreads for {}.", lockManager);
      testPoolManagerWithMultipleThread(lockManager, 1000, 5000);
    }
  }

  private void testPoolManagerWithMultipleThread(LockManager<String> lockManager,
      int threadCount, int loopCount) throws InterruptedException {
    ExecutorService executors = Executors.newFixedThreadPool(threadCount);
    List<Worker> workerList = new ArrayList<>();

    ArrayList<String> lockKeys = new ArrayList<>();
    for (int i = 0; i < 10000; i++) {
      lockKeys.add(String.valueOf(i));
    }
    for (int i = 0; i < threadCount; i++) {
      workerList.add(new Worker(lockManager, loopCount, lockKeys));
    }

    executors.invokeAll(workerList);

    if (!(lockManager instanceof CacheAllLockManager)) {
      Assert.assertEquals(0, lockManager.getNumberOfLocks());
    }

    executors.shutdown();
    executors.shutdownNow();
  }

  static class Worker implements Callable<Void> {
    final LockManager<String> lockManager;
    final int loopCount;
    final ArrayList<String> lockKeys;

    Worker(LockManager<String> lockManager, int loopCount, ArrayList<String> lockKeys) {
      this.lockManager = lockManager;
      this.loopCount = loopCount;
      this.lockKeys = lockKeys;
    }

    @Override
    public Void call() throws Exception {
      for (int i = 0; i < loopCount; i++) {
        int index = ThreadLocalRandom.current().nextInt(10000);
        String lockKey = lockKeys.get(index);
        boolean acquireWriteLock = index <= 3000;
        AutoCloseNNLock<String> lock;
        if (acquireWriteLock) {
          lock = lockManager.acquireWriteLock(lockKey);
        } else {
          lock = lockManager.acquireReadLock(lockKey);
        }

        if (index <= 1000) {
          lock = lockManager.changeWriteLockToReadLock(lock);
        } else if (index >= 9000) {
          lock = lockManager.changeReadLockToWriteLock(lock);
        }

        lock.close();
      }
      return null;
    }
  }

  @Test
  public void testPerformanceForManagers() throws Exception {
    int threadCount = 1000;
    int loopCount = 50000;
    ArrayList<Pair<LockManager<String>, Long>> results = new ArrayList<>();
    for (LockManager<String> lockManager : this.lockManagers) {
      long startTime = Time.monotonicNow();
      testPoolManagerWithMultipleThread(lockManager, threadCount, loopCount);
      long endTime = Time.monotonicNow();
      long costTime = endTime - startTime;

      results.add(Pair.of(lockManager, costTime));
    }

    results.forEach(k ->
        System.out.println(k.fst.getClass().getName() + " costs " + k.snd + "(ms)"));
  }
}
