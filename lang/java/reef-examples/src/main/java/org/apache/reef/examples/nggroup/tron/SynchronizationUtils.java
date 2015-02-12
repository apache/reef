/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.reef.examples.nggroup.tron;

import org.apache.reef.io.network.nggroup.api.GroupChanges;
import org.apache.reef.io.network.nggroup.api.task.CommunicationGroupClient;

import java.util.logging.Level;
import java.util.logging.Logger;

public class SynchronizationUtils {

  private static final Logger LOG = Logger.getLogger(SynchronizationUtils.class.getName());

  public static boolean chkAndUpdate(final CommunicationGroupClient communicationGroupClient) {

    long t1 = System.currentTimeMillis();
    final GroupChanges changes = communicationGroupClient.getTopologyChanges();
    long t2 = System.currentTimeMillis();
    LOG.log(Level.INFO, "OUT: Time to get TopologyChanges = {0} sec", (t2 - t1) / 1000.0);

    if (changes.exist()) {
      LOG.log(Level.INFO, "OUT: There exist topology changes. Asking to update Topology");
      t1 = System.currentTimeMillis();
      communicationGroupClient.updateTopology();
      t2 = System.currentTimeMillis();
      LOG.log(Level.INFO, "OUT: Time to get TopologyUpdated = {0} sec", (t2 - t1) / 1000.0);
      return true;
    } else {
      LOG.log(Level.INFO, "OUT: No changes in topology exist. So not updating topology");
      return false;
    }
  }
}
