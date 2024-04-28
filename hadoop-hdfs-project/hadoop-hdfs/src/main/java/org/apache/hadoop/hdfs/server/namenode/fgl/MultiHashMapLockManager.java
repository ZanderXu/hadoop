package org.apache.hadoop.hdfs.server.namenode.fgl;

import com.google.common.base.Joiner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class MultiHashMapLockManager<T> extends LockManager<T> {
  /** A lock pool to manager all using lock instances. **/
  public final ArrayList<HashMap<T, LockInstance<T>>> lockPools;

  public MultiHashMapLockManager(int capacity, boolean enableLockTrace, boolean isFair) {
    super(enableLockTrace, isFair);
    this.lockPools = new ArrayList<>();
    for (int i = 0; i < capacity; i++) {
      lockPools.add(new HashMap<>(8));
    }
  }

  public void cleanLocks() {
    lockPools.forEach(HashMap::clear);
    lockPools.clear();
  }

  public int getNumberOfLocks() {
    AtomicInteger numberOfLocks = new AtomicInteger();
    lockPools.forEach(k -> numberOfLocks.addAndGet(k.size()));
    return numberOfLocks.get();
  }

  @Override
  protected LockInstance<T> getLockInstanceFromPool(T lockKey) {
    HashMap<T, LockInstance<T>> hashMap = getMap(lockKey);

    synchronized (hashMap) {
      LockInstance<T> lockInstance = hashMap.get(lockKey);
      if (lockInstance == null) {
        lockInstance = new LockInstance<>(lockKey, fair, this);
        hashMap.put(lockKey, lockInstance);
      }

      lockInstance.referenceIncAndGet();
      return lockInstance;
    }
  }

  private HashMap<T, LockInstance<T>> getMap(T lockKey) {
    return this.lockPools.get(
        Math.abs(lockKey.hashCode()) % this.lockPools.size());
  }

  @Override
  public void tryReleaseLockFromPool(T lockKey) {
    HashMap<T, LockInstance<T>> hashMap = getMap(lockKey);

    synchronized (hashMap) {
      LockInstance<T> lockInstance = hashMap.get(lockKey);
      if (lockInstance == null) {
        LOG.warn("Cannot find lock instance belonging to {} from lock pool.", lockKey);
      } else {
        int referenceCounting = lockInstance.referenceDecAndGet();
        if (referenceCounting < 0) {
          LOG.warn("Found an invalid lock instance {} belonging to {} {}. Trace is {}.",
              lockInstance, lockKey, lockKey.hashCode(),
              Joiner.on("\n").join(Thread.currentThread().getStackTrace()));
          hashMap.remove(lockKey);
        } else if (referenceCounting == 0) {
          hashMap.remove(lockKey);
        }
      }
    }
  }
}
