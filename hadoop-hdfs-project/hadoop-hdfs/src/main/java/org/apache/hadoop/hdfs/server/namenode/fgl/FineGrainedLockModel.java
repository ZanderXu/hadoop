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
import java.util.function.Supplier;

/**
 * Using FSLock and BMLock to make namenode thread safe. So there are several common lock groups:
 * 1. Read FSLock: Using for the operations that only getting some information for Directory Tree, such as: getFileInfo
 * 2. Write FSLock: Using for the operations that only modifying some Directory Tree, such as: Mkdirs
 * 3. Read BMLock: Using for the operations that only getting some information for Block or DN, such as: HeartBeat
 * 4. Write BMLock: Using for the operations that only modifying some Blocks or some DNs, such as: RegisterDataNode
 * 5. Read FSLock and Read BMLock: Using for the operations that getting some information
 * 6. Read FSLock and Write BMLock: Using for the operations that modifying some Blocks or some DNs according some information of Directory Tree, such as: RefreshNodes
 * 7. Write FSLock and Read BMLock: Using for the operations that modifying Directory tree according some information of DN or Block.
 * 8. Write FSLock and Write BMLock: Using for the operations that modifying some Blocks or some DNs and Directory tree at the same time, such as: BlockReport
 */
public class FineGrainedLockModel extends AbstractLockModel {

  public FineGrainedLockModel(Configuration conf,
      MutableRatesWithAggregation detailedHoldTimeMetrics) {
    super(new BasicMeasurableLock(conf, "FS", detailedHoldTimeMetrics),
        new BasicMeasurableLock(conf, "BM", detailedHoldTimeMetrics));
  }
}
