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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A lock instance contains Read/Write lock and reference counting.
 * You can get an AutoCloseReadLock or AutoCloseWriteLock from this instance.
 * If the reference counting is not greater than 0, this instance may be deleted
 * from memory by LockManager.
 */
public class LockInstance<T> {

  /** The reference counting for tracing this lock instance. **/
  private final AtomicInteger referenceCount = new AtomicInteger(0);

  /** The lock key that this lock instance belongs to. **/
  private final T lockKey;

  /** The reentrant read-write lock for this lock key. **/
  private final ReentrantReadWriteLock rwLock;

  /** Cached autocloseable read lock. **/
  private final AutoCloseNNLock<T> readLock;

  /** Cached autocloseable write lock. **/
  private final AutoCloseNNLock<T> writeLock;

  public LockInstance(T lockKey, boolean fair, LockManager<T> lockManager) {
    this.rwLock = new ReentrantReadWriteLock(fair);
    this.readLock = new AutoCloseNNLock<>(rwLock.readLock(), this);
    this.readLock.setLockManager(lockManager);
    this.writeLock = new AutoCloseNNLock<>(rwLock.writeLock(), this);
    this.writeLock.setLockManager(lockManager);
    this.lockKey = lockKey;
  }

  public AutoCloseNNLock<T> getReadLock() {
    return this.readLock;
  }

  public AutoCloseNNLock<T> getWriteLock() {
    return this.writeLock;
  }

  public T getLockKey() {
    return this.lockKey;
  }

  public int referenceIncAndGet() {
    return this.referenceCount.incrementAndGet();
  }

  public int referenceDecAndGet() {
    return this.referenceCount.decrementAndGet();
  }

  public int getReferenceCount() {
    return this.referenceCount.get();
  }

  /**
   * Return true if the reentrant read holds on this lock by the current thread.
   */
  public boolean isReadLockHeldByCurrentThread() {
    return this.rwLock.getReadHoldCount() > 0;
  }

  public boolean isWriteLockHeldByCurrentThread() {
    return this.rwLock.isWriteLockedByCurrentThread();
  }

  @Override
  public String toString() {
    return lockKey.toString() + ", referenceCount=" + referenceCount.get();
  }
}
