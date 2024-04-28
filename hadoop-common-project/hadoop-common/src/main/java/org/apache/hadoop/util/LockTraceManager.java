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

package org.apache.hadoop.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

/**
 * A class for checking deadlocks by tracing threads holding this lock.
 */
public class LockTraceManager {
  private final static Logger LOG = LoggerFactory.getLogger(LockTraceManager.class);

  private final HashMap<String, TrackLog> threadCountMap = new HashMap<>();

  private Exception lastException;

  /**
   * Add thread name when lock a lock.
   */
  public synchronized void putThreadName() {
    String thread = getThreadName();
    if (threadCountMap.containsKey(thread)) {
      TrackLog trackLog = threadCountMap.get(thread);
      trackLog.incrLockCount();
    }

    if (!threadCountMap.containsKey(thread)) {
      synchronized (threadCountMap) {
        if (!threadCountMap.containsKey(thread)) {
          threadCountMap.put(thread, new TrackLog(thread));
        }
      }
    }
  }

  /**
   * Remove thread name when unlock a lock.
   */
  public synchronized void removeThreadName() {
    String thread = getThreadName();
    if (threadCountMap.containsKey(thread)) {
      TrackLog trackLog = threadCountMap.get(thread);
      if (trackLog.shouldClear()) {
        threadCountMap.remove(thread);
        return;
      }
      trackLog.decrLockCount();
    }
  }

  /**
   * Class for record thread acquire lock stack trace and count.
   */
  private static class TrackLog {
    private final Stack<Exception> logStack = new Stack<>();
    private int lockCount = 0;
    private final String threadName;

    TrackLog(String threadName) {
      this.threadName = threadName;
      incrLockCount();
    }

    public void incrLockCount() {
      logStack.push(new Exception("lock stack trace"));
      lockCount += 1;
    }

    public void decrLockCount() {
      logStack.pop();
      lockCount -= 1;
    }

    public void showLockMessage() {
      LOG.error("hold lock thread name is: {} hold count is: {}", threadName, lockCount);
      while (!logStack.isEmpty()) {
        Exception e = logStack.pop();
        LOG.error("lock stack ", e);
      }
    }

    public boolean shouldClear() {
      return lockCount == 1;
    }
  }

  public synchronized void lockLeakCheck() {
    if (threadCountMap.isEmpty()) {
      LOG.info("all lock has release");
      return;
    }
    setLastException(new Exception("lock Leak"));
    for (Map.Entry<String, TrackLog> entry : threadCountMap.entrySet()) {
      entry.getValue().showLockMessage();
    }
  }

  private void setLastException(Exception e) {
    this.lastException = e;
  }

  public Exception getLastException() {
    return lastException;
  }

  private String getThreadName() {
    return Thread.currentThread().getName() + Thread.currentThread().getId();
  }
}
