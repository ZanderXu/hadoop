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

import org.apache.hadoop.util.LockTraceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class for managing some lock instances, such as: acquiring lock, releasing lock.
 *
 * @param <T> T is a lock key to acquire the unique lock instance.
 */
public abstract class LockManager<T> {

  protected final Logger LOG = LoggerFactory.getLogger(LockManager.class);

  /** A manager for tracing locks. **/
  private final LockTraceManager lockTraceManager;

  protected final boolean fair;

  protected LockManager(boolean enableLockTrace, boolean isFair) {
    if (enableLockTrace) {
      this.lockTraceManager = new LockTraceManager();
    } else {
      this.lockTraceManager = null;
    }

    this.fair = isFair;
  }

  /**
   * Do something after auto-closing lock is closed.
   */
  public void hook(T lockKey) {
    tryReleaseLockFromPool(lockKey);
    if (lockTraceManager != null) {
      lockTraceManager.removeThreadName();
    }
  }

  /**
   * Acquire an auto-closing read lock belonging to the input lock key.
   * @param lockKey lock key.
   * @return auto-closing read lock.
   */
  public AutoCloseNNLock<T> acquireReadLock(T lockKey) {
    if (lockKey == null) {
      throw new IllegalArgumentException("The lock key should not be null.");
    }

    LockInstance<T> instance = getLockInstanceFromPool(lockKey);
    AutoCloseNNLock<T> autoCloseNNLock = instance.getReadLock();

    lockWithMetrics(autoCloseNNLock);

    traceLock();
    return autoCloseNNLock;
  }

  /**
   * Acquire an auto-closing write lock belonging to the input lock key.
   * @param lockKey lock key.
   * @return auto-closing write lock.
   */
  public AutoCloseNNLock<T> acquireWriteLock(T lockKey) {
    if (lockKey == null) {
      throw new IllegalArgumentException("The lock key should not be null.");
    }

    LockInstance<T> instance = getLockInstanceFromPool(lockKey);

    // If the current thread already holds the read lock for this lock key,
    // the thread will be blocked when acquiring the write lock. It results in deadlock.
    // So here should throw Exception to avoid it.
    if (instance.isReadLockHeldByCurrentThread()) {
      // Try to release this lock from pool, since getLockInstanceFromPool already
      // increased the reference count of this lock instance.
      tryReleaseLockFromPool(lockKey);
      throw new IllegalStateException(
          "Thread attempts to acquire the write lock while holding the read lock");
    }

    AutoCloseNNLock<T> autoCloseNNLock = instance.getWriteLock();

    lockWithMetrics(autoCloseNNLock);
    traceLock();

    return autoCloseNNLock;
  }

  /**
   * Try to change the holding read lock to write lock for the input lock key.
   * @param readLock current holding read lock.
   * @return one write lock.
   */
  public AutoCloseNNLock<T> changeReadLockToWriteLock(AutoCloseNNLock<T> readLock) {
    if (!readLock.isReadLockHoldByCurrentThread()) {
      throw new IllegalMonitorStateException(
          "The lock is not read lock or not held by the current thread.");
    }

    LockInstance<T> instance = readLock.getLockInstance();

    // close the holding read lock, but don't release its instance.
    readLock.close(false);

    // Acquire the write lock.
    AutoCloseNNLock<T> autoCloseNNLock = instance.getWriteLock();
    lockWithMetrics(autoCloseNNLock);

    return autoCloseNNLock;
  }

  /**
   * Try to change the holding write lock to read lock for the input lock key.
   * @param writeLock current holding write lock.
   * @return one read lock.
   */
  public AutoCloseNNLock<T> changeWriteLockToReadLock(AutoCloseNNLock<T> writeLock) {
    if (!writeLock.isWriteLockHeldByCurrentThread()) {
      throw new IllegalMonitorStateException(
          "The lock is not write lock or not held by the current thread.");
    }
    LockInstance<T> instance = writeLock.getLockInstance();

    // Since the current thread is holding its write lock, it can acquire the read lock immediately.
    AutoCloseNNLock<T> autoCloseNNLock = instance.getReadLock();
    lockWithMetrics(autoCloseNNLock);

    // close the holding write lock without release it.
    writeLock.close(false);

    return autoCloseNNLock;
  }

  protected void lockWithMetrics(AutoCloseNNLock<T> autoCloseNNLock) {
    // TODO: add some metrics for this lock.
    autoCloseNNLock.lock();
  }

  protected void traceLock() {
    if (lockTraceManager != null) {
      lockTraceManager.putThreadName();
    }
  }

  public void close() {
   cleanLocks();

   if (lockTraceManager != null) {
     lockTraceManager.lockLeakCheck();
   }
  }

  abstract void cleanLocks();

  public abstract int getNumberOfLocks();

  /**
   * Get one lock instance belonging to the lock key from lock pool.
   */
  abstract LockInstance<T> getLockInstanceFromPool(T lockKey);

  /**
   * Release this lock from pool.
   */
  abstract void tryReleaseLockFromPool(T lockKey);
}
