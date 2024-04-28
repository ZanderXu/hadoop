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

import java.util.concurrent.ConcurrentHashMap;

public class CacheAllLockManager<T> extends LockManager<T> {

  /** A lock pool to manager all using lock instances. **/
  public final ConcurrentHashMap<T, LockInstance<T>> lockPool;

  public CacheAllLockManager(int capacity, boolean enableLockTrace, boolean isFair) {
    super(enableLockTrace, isFair);
    this.lockPool = new ConcurrentHashMap<>(capacity);
  }

  public void cleanLocks() {
    this.lockPool.clear();
  }

  public int getNumberOfLocks() {
    return this.lockPool.size();
  }

  @Override
  protected LockInstance<T> getLockInstanceFromPool(T lockKey) {
    LockInstance<T> lockInstance = lockPool.get(lockKey);
    if (lockInstance != null) {
      return lockInstance;
    } else {
      return lockPool.compute(lockKey, (key, instance) -> {
        if (instance != null) {
          return instance;
        } else {
          return new LockInstance<>(lockKey, fair, this);
        }
      });
    }
  }

  @Override
  public void tryReleaseLockFromPool(T lockKey) {
    // do nothing.
  }
}
