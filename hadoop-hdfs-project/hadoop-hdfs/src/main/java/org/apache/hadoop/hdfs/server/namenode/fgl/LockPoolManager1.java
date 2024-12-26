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
package org.apache.hadoop.hdfs.server.namenode.fgl;

import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A lock manager that supports various lock operations for a key class {@link K}. These operations
 * include (but are not limited to) acquisition, release, leak check, emptying and closure.
 *
 * @param <K> key for the lock manager
 */
public class LockPoolManager1<K> {
  private static final Logger LOG = LoggerFactory.getLogger(LockPoolManager1.class);

  // All locks that be used by K.
  private final ConcurrentHashMap<K, LockResource1> locks;
  // All available locks.
  private final ConcurrentLinkedQueue<LockResource1> availableLocks;
  private final long locksCapacity;

  private final LockTraceManager1 lockTraceManager;
  private final boolean openLockTrace;
  private final AvailableLockMonitor monitor;

  public LockPoolManager1(int locksCapacity, boolean openLockTrace) {
    this.locks = new ConcurrentHashMap<>(locksCapacity);
    this.availableLocks = new ConcurrentLinkedQueue<>();
    this.locksCapacity = locksCapacity;
    this.openLockTrace = openLockTrace;
    this.lockTraceManager = new LockTraceManager1();
    this.monitor = new AvailableLockMonitor();
    this.monitor.setName("LockPoolManager_AvailableLockMonitor");
    this.monitor.start();

    LOG.info(
        "Initializing LockPoolManager with locksCapacity={}, openLockTrace={}.",
        locksCapacity, openLockTrace);
  }


  /**
   * Attempts to acquire a lock for the given lock key and stores in {@link LockPoolManager1#locks}.
   */
  public AutoCloseableLockInPool1<K> acquireLock(K key, LockMode mode) {
    return acquireLockInternal(key, mode);
  }

  /**
   * Acquires an {@link AutoCloseableLockInPool1} for the given lock key.
   */
  private AutoCloseableLockInPool1<K> acquireLockInternal(K key, LockMode mode) {
    if (key == null) {
      throw new IllegalArgumentException("The key shouldn't be null");
    }
    LockResource1 lock = acquireLockResource(key);

    // AutoCloseableLockInPool objects are not directly stored
    // because there are some cases that the lock mode will be changed.
    AutoCloseableLockInPool1<K> autoCloseableLockInPool =
        new AutoCloseableLockInPool1<>(this, key, lock, mode);

    if (openLockTrace) {
      lockTraceManager.putThreadName();
    }
    return autoCloseableLockInPool;
  }

  private LockResource1 acquireLockResource(K key) {
    return this.locks.compute(key, (k, v) -> {
      if (v != null) {
        v.ref.incrementAndGet();
        return v;
      } else {
        LockResource1 lock = availableLocks.poll();
        if (lock == null) {
          lock = new LockResource1();
        }
        lock.ref.incrementAndGet();
        return lock;
      }
    });
  }

  /**
   * Releases the lock for the given lock key.
   * If cached, just decrements the ref count. Else, attempts to release the lock from
   * the universal lock pool.
   *
   * @param key lock key to release
   */
  public void releaseLockResource(K key) {
    this.locks.get(key).ref.decrementAndGet();
  }

  public void releaseLockResourceAndRemoveFromLockTrace(K lockKey) {
    releaseLockResource(lockKey);
    if (openLockTrace) {
      lockTraceManager.removeThreadName();
    }
  }

  public void close() throws InterruptedException {
    this.monitor.shutdown();
    this.monitor.join();
    for (Map.Entry<K, LockResource1> entry : this.locks.entrySet()) {
      if (entry.getValue().ref.get() != 0) {
        LOG.warn("LockInstance of {} is still hold by {} thread.", entry.getKey(), entry.getValue().ref.get());
      }
    }

    locks.clear();
    this.availableLocks.clear();
    checkForLockLeak();
  }

  /**
   * Checks for leak.
   */
  private void checkForLockLeak() {
    if (!openLockTrace) {
      LOG.warn("Lock trace disabled.");
      return;
    }
    lockTraceManager.checkForLeak();
  }

  public enum LockMode {
    READ, WRITE
  }

  /**
   * A class that periodically scans the lock pool and replaces the cache every hour
   * with the most active locks.
   */
  class AvailableLockMonitor extends Thread {
    private volatile boolean shouldRun = true;
    public AvailableLockMonitor() {
    }

    public void shutdown() {
      this.shouldRun = false;
    }

    @Override
    public void run() {
      while (shouldRun) {
        try {
          AtomicReference<LockResource1> lockResourceRef = new AtomicReference<>();
          for(Map.Entry<K, LockResource1> entry : locks.entrySet()) {
            if (entry.getValue().ref.get() == 0) {
              K lockKey = entry.getKey();
              LockResource1 lockResource = locks.computeIfPresent(lockKey, (k, v) -> {
                if (v.ref.get() == 0) {
                  lockResourceRef.set(v);
                  return null;
                } else {
                  return v;
                }
              });

              if (lockResource == null && lockResourceRef.get() != null) {
                availableLocks.add(lockResourceRef.get());
                LOG.debug("Get available lock {} used by {} before.", lockResourceRef.get(), entry.getKey());
                lockResourceRef.set(null);
              }
            }
          }

          Thread.sleep(10);
        } catch (InterruptedException e) {
          LOG.error("Interrupted", e);
          shouldRun = false;
        } catch (Throwable t) {
          shouldRun = false;
          LOG.error("Unexpected error while waiting for lock monitor.", t);
        }
      }
    }
  }

  public void testPerformanceByMultipleThreads(K[] keys, int threadNumber, int countPerThread)
      throws Exception {
    ThreadPoolExecutor executor = new ThreadPoolExecutor(threadNumber, threadNumber,
        0, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
    final AtomicLong timeDuration = new AtomicLong();
    ArrayList<Future<?>> futures = new ArrayList<>();
    for (int i = 0; i < threadNumber; i++) {
      Future<?> future = executor.submit(() -> {
        AtomicLong duration = new AtomicLong();
        for (int j = 0; j < countPerThread; j++) {
          K lockKey = keys[ThreadLocalRandom.current().nextInt(keys.length)];
          LockMode mode = ThreadLocalRandom.current().nextBoolean() ? LockMode.READ : LockMode.WRITE;

          LOG.debug("Acquire {} lock for {} by thread {} in loop {}.",
              lockKey, mode, Thread.currentThread().getName(), j);
          long startTime = Time.monotonicNow();
          AutoCloseableLockInPool1<K> lock = acquireLock(lockKey, mode);
          lock.close();
          duration.addAndGet((Time.monotonicNow() - startTime));
        }

        LOG.info("Acquire and release {} locks takes {}(ms).", countPerThread, duration.get());
        timeDuration.addAndGet(duration.get());
      });
      futures.add(future);
    }

    for (Future<?> future : futures) {
      future.get();
    }

    LOG.info("testPerformanceByMultipleThreads cost {}ms, the number of keys is {}, " +
        "the number of threads is {}, the count of per thread is {}.", timeDuration.get(),
        keys.length, threadNumber, countPerThread);
  }

  public static void main(String[] args) throws Exception {
    int locksCapacity = Integer.parseInt(args[0]);
    boolean openLockTrace = Boolean.parseBoolean(args[1]);
    int locksCount = Integer.parseInt(args[2]);
    int threadCount = Integer.parseInt(args[3]);
    int countPerThread = Integer.parseInt(args[4]);

    LockPoolManager1<String>
        lockPoolManager = new LockPoolManager1<>(locksCapacity, openLockTrace);

    String[] lockKeys = new String[locksCount];
    for (int i = 0; i < lockKeys.length; i++) {
      lockKeys[i] = String.valueOf(i);
    }

    long startTime = System.currentTimeMillis();

    lockPoolManager.testPerformanceByMultipleThreads(lockKeys,
        threadCount, countPerThread);

    long endTime = System.currentTimeMillis();
    LOG.info("testPerformanceByMultipleThreads cost {}ms, the number of keys is {}, " +
            "the number of threads is {}, the count of per thread is {}.", (endTime - startTime),
        locksCount, threadCount, countPerThread);


    lockPoolManager.close();

    System.exit(0);
  }
}