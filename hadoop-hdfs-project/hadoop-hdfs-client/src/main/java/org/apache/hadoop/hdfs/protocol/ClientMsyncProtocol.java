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
package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSelector;
import org.apache.hadoop.hdfs.server.namenode.ha.ReadOnly;
import org.apache.hadoop.io.retry.Idempotent;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.token.TokenInfo;

import java.io.IOException;

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY;

/**********************************************************************
 * ClientMsyncProtocol is used by ObserverReadProxyProvider or RBF to
 * get last txid from the NameNode.
 **********************************************************************/
@InterfaceAudience.Private
@InterfaceStability.Evolving
@KerberosInfo(serverPrincipal = DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY)
@TokenInfo(DelegationTokenSelector.class)
public interface ClientMsyncProtocol {

  /**
   * same as {@link ClientProtocol#msync()}
   */
  @Idempotent
  @ReadOnly(activeOnly = true)
  void msync() throws IOException;
}
