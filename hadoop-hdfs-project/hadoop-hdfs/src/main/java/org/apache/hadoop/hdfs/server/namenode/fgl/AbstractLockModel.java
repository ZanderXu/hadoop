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

import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

public class AbstractLockModel {

  private final BasicMeasurableLock fsLock;
  private final BasicMeasurableLock bmLock;

  public AbstractLockModel(BasicMeasurableLock fsLock, BasicMeasurableLock bmLock) {
    this.fsLock = fsLock;
    this.bmLock = bmLock;
  }

  public AbstractLockModel(BasicMeasurableLock glLock) {
    this.fsLock = glLock;
    this.bmLock = glLock;
  }

  public BasicMeasurableLock getFsLock() {
    return this.fsLock;
  }

  public void readLock() {
    this.fsLock.readLock();
    this.bmLock.readLock();
  }

  public void readLockInterruptibly() throws InterruptedException {
    this.fsLock.readLockInterruptibly();
    try {
      this.bmLock.readLockInterruptibly();
    } catch (InterruptedException e) {
      this.fsLock.readUnlock();
      throw e;
    }
  }

  public void readUnlock() {
    this.bmLock.readUnlock();
    this.fsLock.readUnlock();
  }

  public void readUnlock(String opName) {
    this.bmLock.readUnlock(opName);
    this.fsLock.readUnlock(opName);
  }

  public void readUnlock(String opName, Supplier<String> lockReportInfoSupplier) {
    this.bmLock.readUnlock(opName, lockReportInfoSupplier);
    this.fsLock.readUnlock(opName, lockReportInfoSupplier);
  }

  public void writeLock() {
    this.fsLock.writeLock();
    this.bmLock.writeLock();
  }

  public void writeLockInterruptibly() throws InterruptedException {
    this.fsLock.writeLockInterruptibly();
    try {
      this.bmLock.writeLockInterruptibly();
    } catch (InterruptedException e) {
      this.fsLock.writeUnlock();
      throw e;
    }
  }

  public void writeUnlock() {
    this.bmLock.writeUnlock();
    this.fsLock.writeUnlock();
  }

  public void writeUnlock(String opName) {
    this.bmLock.writeUnlock(opName);
    this.fsLock.writeUnlock(opName);
  }

  public void writeUnlock(String opName, boolean suppressWriteLockReport) {
    this.bmLock.writeUnlock(opName, suppressWriteLockReport);
    this.fsLock.writeUnlock(opName, suppressWriteLockReport);
  }

  public void writeUnlock(String opName, Supplier<String> lockReportInfoSupplier) {
    this.bmLock.writeUnlock(opName, lockReportInfoSupplier);
    this.fsLock.writeUnlock(opName, lockReportInfoSupplier);
  }

  public boolean hasWriteLock() {
    return this.fsLock.isWriteLockedByCurrentThread() && this.bmLock.isWriteLockedByCurrentThread();
  }

  public boolean hasReadLock() {
    return hasFSReadLock() && hasBMReadLock();
  }

  public boolean hasFSReadLock() {
    return this.fsLock.getReadHoldCount() > 0 || this.fsLock.isWriteLockedByCurrentThread();
  }

  public boolean hasFSWriteLock() {
    return this.fsLock.isWriteLockedByCurrentThread();
  }

  public boolean hasBMReadLock() {
    return this.bmLock.getReadHoldCount() > 0 || this.bmLock.isWriteLockedByCurrentThread();
  }

  public boolean hasBMWriteLock() {
    return this.bmLock.isWriteLockedByCurrentThread();
  }

  public int getReadHoldCount() {
    return -1;
  }

  public int getWriteHoldCount() {
    return -1;
  }

  public void readFSLock() {
    this.fsLock.readLock();
  }

  public void readFSUnLock(String opName) {
    this.fsLock.readUnlock(opName);
  }

  public void writeFSLock() {
    this.fsLock.writeLock();
  }

  public void writeFSUnlock(String opName) {
    this.fsLock.writeUnlock(opName);
  }

  public void writeFSUnlock(String opName, Supplier<String> lockReportInfoSupplier) {
    this.fsLock.writeUnlock(opName, lockReportInfoSupplier);
  }

  public void writeFSUnlock(String opName, boolean suppressWriteLockReport) {
    this.fsLock.writeUnlock(opName, suppressWriteLockReport);
  }

  public void readBMLock() {
    this.bmLock.readLock();
  }

  public void readBMUnLock(String opName) {
    this.bmLock.readUnlock(opName);
  }

  public void writeBMLock() {
    this.bmLock.writeLock();
  }

  public void writeBMUnLock(String opName) {
    this.bmLock.writeUnlock(opName);
  }

  public void readFSLockAndWriteBMLock() {
    this.fsLock.readLock();
    this.bmLock.writeLock();
  }

  public void readFSUnLockAndWriteBMUnLock(String opName) {
    this.bmLock.writeUnlock(opName);
    this.fsLock.readUnlock(opName);
  }

  public void writeFSLockAndReadBMLock() {
    this.fsLock.writeLock();
    this.bmLock.readLock();
  }

  public void writeFSUnLockAndReadBMUnLock(String opName) {
    this.bmLock.readUnlock(opName);
    this.fsLock.writeUnlock(opName);
  }

  public void setMetricsEnabled(boolean metricsEnabled) {
    this.fsLock.setMetricsEnabled(metricsEnabled);
    this.bmLock.setMetricsEnabled(metricsEnabled);
  }

  public int getQueueLength() {
    return -1;
  }

  public long getNumOfReadLockLongHold() {
    return -1;
  }

  public long getNumOfWriteLockLongHold() {
    return -1;
  }

  @VisibleForTesting
  public void setLockForTests(ReentrantReadWriteLock lock) {

  }

  @VisibleForTesting
  public ReentrantReadWriteLock getLockForTests() {
    return null;
  }


  @VisibleForTesting
  public boolean isMetricsEnabled() {
    return this.fsLock.isMetricsEnabled();
  }

  public void setReadLockReportingThresholdMs(long readLockReportingThresholdMs) {
    this.fsLock.setReadLockReportingThresholdMs(readLockReportingThresholdMs);
    this.bmLock.setReadLockReportingThresholdMs(readLockReportingThresholdMs);
  }

  @VisibleForTesting
  public long getReadLockReportingThresholdMs() {
    return this.fsLock.getReadLockReportingThresholdMs();
  }

  public void setWriteLockReportingThresholdMs(long writeLockReportingThresholdMs) {
    this.fsLock.setWriteLockReportingThresholdMs(writeLockReportingThresholdMs);
    this.bmLock.setWriteLockReportingThresholdMs(writeLockReportingThresholdMs);
  }

  @VisibleForTesting
  public long getWriteLockReportingThresholdMs() {
    return this.fsLock.getWriteLockReportingThresholdMs();
  }
}
