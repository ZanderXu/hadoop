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

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestLockPoolManager {

  @Test
  public void testAcquireNonCachedLockFromManager1() {
    int loopCount = 1000000;
    LockPoolManager1<String> lockPoolManager1 = new LockPoolManager1<>(loopCount, false);

    long startTime = System.nanoTime();
    for (int i = 0; i < loopCount; i++) {
      AutoCloseableLockInPool1 lock = lockPoolManager1.acquireLock(Integer.toString(i),
          LockPoolManager1.LockMode.READ);
    }
    long endTime = System.nanoTime();
    System.out.println("AcquireNonCachedLock from Manager1: " + (endTime - startTime) + " ns");
  }

  @Test
  public void testAcquireNonCachedLockFromManager() {
    int loopCount = 1000000;
    LockPoolManager<String> lockPoolManager = new LockPoolManager<>(loopCount, loopCount, false);
    long startTime1 = System.nanoTime();
    for (int i = 0; i < loopCount; i++) {
      AutoCloseableLockInPool lock = lockPoolManager.acquireLock(Integer.toString(i),
          LockPoolManager.LockMode.READ);
    }
    long endTime1 = System.nanoTime();
    System.out.println("AcquireNonCachedLock from Manager: " + (endTime1 - startTime1) + " ns");
  }

  @Test
  public void testAcquireCachedLockFromManager1() {
    int capacity = 100000;
    int loopCount = 1000000;
    LockPoolManager1<String> lockPoolManager1 = new LockPoolManager1<>(capacity, false);

    for (int i = 0; i < capacity; i++) {
      // Cache locks for some keys
      lockPoolManager1.acquireLock(Integer.toString(i), LockPoolManager1.LockMode.READ);
    }

    long startTime = System.nanoTime();
    for (int i = 0; i < loopCount; i++) {
      // Acquire some cached locks.
      lockPoolManager1.acquireLock(Integer.toString(i%capacity), LockPoolManager1.LockMode.READ);
    }
    long endTime = System.nanoTime();
    System.out.println("AcquireCachedLock from Manager1: " + (endTime - startTime) + " ns");
  }

  @Test
  public void testAcquireCachedLockFromManager() {
    int capacity = 100000;
    int loopCount = 1000000;

    LockPoolManager<String> lockPoolManager = new LockPoolManager<>(capacity, capacity, false);
    for (int i = 0; i < capacity; i++) {
      AutoCloseableLockInPool lock = lockPoolManager.acquireAndCacheLock(Integer.toString(i),
          LockPoolManager.LockMode.READ);
    }

    long startTime1 = System.nanoTime();
    for (int i = 0; i < loopCount; i++) {
      // Acquire some cached locks.
      lockPoolManager.acquireLock(Integer.toString(i%capacity), LockPoolManager.LockMode.READ);
    }
    long endTime1 = System.nanoTime();
    System.out.println("AcquireCachedLock from Manager: " + (endTime1 - startTime1) + " ns");
  }

  @Test
  public void testReleaseUsedLockFromManager1() {
    int loopCount = 1000000;
    LockPoolManager1<String> lockPoolManager1 = new LockPoolManager1<>(loopCount * 2, false);
    List<AutoCloseableLockInPool1<String>> locks = new ArrayList<>();
    for (int i = 0; i < loopCount; i++) {
      AutoCloseableLockInPool1<String> lock = lockPoolManager1.acquireLock(Integer.toString(i),
          LockPoolManager1.LockMode.READ);
      locks.add(lock);
    }

    long startTime = System.nanoTime();
    for (AutoCloseableLockInPool1<String> lock : locks) {
      lock.close();
    }
    long endTime = System.nanoTime();
    System.out.println("ReleaseUsedLock from Manager1: " + (endTime - startTime) + " ns");
  }

  @Test
  public void testReleaseUnusedLockFromManager() {
    int loopCount = 1000000;
    LockPoolManager<String> lockPoolManager = new LockPoolManager<>(loopCount, loopCount, false);
    List<AutoCloseableLockInPool<String>> locks = new ArrayList<>();
    for (int i = 0; i < loopCount; i++) {
      AutoCloseableLockInPool<String> lock = lockPoolManager.acquireLock(Integer.toString(i),
          LockPoolManager.LockMode.READ);
      locks.add(lock);
    }

    long startTime = System.nanoTime();
    for (AutoCloseableLockInPool<String> lock : locks) {
      lock.close();
    }
    long endTime = System.nanoTime();
    System.out.println("ReleaseUsedLock from Manager1: " + (endTime - startTime) + " ns");
  }
}
