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

import com.google.common.base.Joiner;

import java.util.concurrent.ConcurrentHashMap;

public class ConcurrentHashMapLockManager<T> extends LockManager<T> {
  /** A lock pool to manager all using lock instances. **/
  public final ConcurrentHashMap<T, LockInstance<T>> lockPool;

  public ConcurrentHashMapLockManager(int capacity, boolean enableLockTrace, boolean isFair) {
    super(enableLockTrace, isFair);
    this.lockPool = new ConcurrentHashMap<>(capacity);
  }

  public void cleanLocks() {
    this.lockPool.clear();
  }

  public int getNumberOfLocks() {
    return this.lockPool.size();
  }

  protected LockInstance<T> getLockInstanceFromPool(T lockKey) {
    return lockPool.compute(lockKey, (key, instance) -> {
      if (instance != null && instance.referenceIncAndGet() > 1) {
        return instance;
      } else {
        LockInstance<T> newInstance = new LockInstance<>(lockKey, fair, this);
        newInstance.referenceIncAndGet();
        return newInstance;
      }
    });
  }

  /**
   * Try to delete the lock instance belonging to lock key from lock pool.
   * @param lockKey lock key
   */
  public void tryReleaseLockFromPool(T lockKey) {
    lockPool.compute(lockKey, (key, instance) -> {
      // lock pool will delete this key if null is returned,
      // else lock pool will update this key with new value.
      if (instance == null) {
        LOG.warn("Cannot find lock instance belonging to {} from lock pool. Trace is {}.",
            lockKey, Joiner.on("\n").join(Thread.currentThread().getStackTrace()));
        return null;
      } else {
        int refValue = instance.referenceDecAndGet();
        if (refValue < 0) {
          LOG.warn("Found an invalid lock instance {} belonging to {} {}. Trace is {}.",
              instance, lockKey, lockKey.hashCode(),
              Joiner.on("\n").join(Thread.currentThread().getStackTrace()));
          return null;
        } else if (refValue == 0) {
          return null;
        } else {
          return instance;
        }
      }
    });
  }
}
