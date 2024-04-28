package org.apache.hadoop.hdfs.server.namenode.fgl;

import com.google.common.base.Joiner;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LockKeyLevelLockManager<T> extends LockManager<T> {
  /** A lock pool to manager all using lock instances. **/
  public final Map<T, LockInstance<T>> lockPool;

  public LockKeyLevelLockManager(int capacity, boolean enableLockTrace, boolean isFair) {
    super(enableLockTrace, isFair);
    this.lockPool = new ConcurrentHashMap<>(capacity);
  }

  public void cleanLocks() {
    this.lockPool.clear();
  }

  public int getNumberOfLocks() {
    return this.lockPool.size();
  }

  /**
   * Get one lock instance belonging to the lock key from lock pool.
   * One new instance will be created if it is not in the pool,
   * else the reference counter will be increased.
   * @param lockKey lock key.
   * @return lock instance belonging to this lock key.
   */
  protected LockInstance<T> getLockInstanceFromPool(T lockKey) {
    synchronized (lockKey) {
      LockInstance<T> lockInstance = lockPool.get(lockKey);
      if (lockInstance == null) {
        lockInstance = new LockInstance<>(lockKey, fair, this);
        lockPool.put(lockKey, lockInstance);
      }

      lockInstance.referenceIncAndGet();
      return lockInstance;
    }
  }

  /**
   * Try to delete the lock instance belonging to lock key from lock pool.
   * @param lockKey lock key
   */
  public void tryReleaseLockFromPool(T lockKey) {
    synchronized (lockKey) {
      LockInstance<T> lockInstance = lockPool.get(lockKey);
      if (lockInstance == null) {
        LOG.warn("Cannot find lock instance belonging to {} from lock pool. Trace is {}.",
            lockKey, Joiner.on("\n").join(Thread.currentThread().getStackTrace()));
      } else {
        int referenceCounting = lockInstance.referenceDecAndGet();
        if (referenceCounting < 0) {
          LOG.warn("Found an invalid lock instance {} belonging to {} {}. Trace is {}.",
              lockInstance, lockKey, lockKey.hashCode(),
              Joiner.on("\n").join(Thread.currentThread().getStackTrace()));
          lockPool.remove(lockKey);
        } else if (referenceCounting == 0) {
          lockPool.remove(lockKey);
        }
      }
    }
  }
}
