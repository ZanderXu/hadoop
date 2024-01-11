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

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.lib.MutableRatesWithAggregation;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public class GlobalLockModel extends AbstractLockModel {

  private final BasicMeasurableLock glLock;

  public GlobalLockModel(Configuration conf, MutableRatesWithAggregation detailedHoldTimeMetrics) {
    super(new BasicMeasurableLock(conf, "FSN", detailedHoldTimeMetrics));
    this.glLock = super.getFsLock();
  }

  public int getQueueLength() {
    return this.glLock.getQueueLength();
  }

  public long getNumOfReadLockLongHold() {
    return this.glLock.getNumOfReadLockLongHold();
  }

  public long getNumOfWriteLockLongHold() {
    return this.glLock.getNumOfWriteLockLongHold();
  }

  @VisibleForTesting
  public void setLockForTests(ReentrantReadWriteLock lock) {
    this.glLock.setLockForTests(lock);
  }

  @VisibleForTesting
  public ReentrantReadWriteLock getLockForTests() {
    return this.glLock.getLockForTests();
  }
}
