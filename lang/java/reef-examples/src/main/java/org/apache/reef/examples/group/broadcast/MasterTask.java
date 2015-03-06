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
package org.apache.reef.examples.group.broadcast;

import org.apache.reef.examples.group.bgd.operatornames.ControlMessageBroadcaster;
import org.apache.reef.examples.group.bgd.parameters.AllCommunicationGroup;
import org.apache.reef.examples.group.bgd.parameters.ModelDimensions;
import org.apache.reef.examples.group.broadcast.parameters.ModelBroadcaster;
import org.apache.reef.examples.group.broadcast.parameters.ModelReceiveAckReducer;
import org.apache.reef.examples.group.utils.math.DenseVector;
import org.apache.reef.examples.group.utils.math.Vector;
import org.apache.reef.io.network.group.api.operators.Broadcast;
import org.apache.reef.io.network.group.api.operators.Reduce;
import org.apache.reef.io.network.group.api.GroupChanges;
import org.apache.reef.io.network.group.api.task.CommunicationGroupClient;
import org.apache.reef.io.network.group.api.task.GroupCommClient;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.Task;
import org.mortbay.log.Log;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MasterTask implements Task {

  public static final String TASK_ID = "MasterTask";

  private static final Logger LOG = Logger.getLogger(MasterTask.class.getName());

  private final CommunicationGroupClient communicationGroupClient;
  private final Broadcast.Sender<ControlMessages> controlMessageBroadcaster;
  private final Broadcast.Sender<Vector> modelBroadcaster;
  private final Reduce.Receiver<Boolean> modelReceiveAckReducer;

  private final int dimensions;

  @Inject
  public MasterTask(
      final GroupCommClient groupCommClient,
      final @Parameter(ModelDimensions.class) int dimensions) {

    this.dimensions = dimensions;

    this.communicationGroupClient = groupCommClient.getCommunicationGroup(AllCommunicationGroup.class);
    this.controlMessageBroadcaster = communicationGroupClient.getBroadcastSender(ControlMessageBroadcaster.class);
    this.modelBroadcaster = communicationGroupClient.getBroadcastSender(ModelBroadcaster.class);
    this.modelReceiveAckReducer = communicationGroupClient.getReduceReceiver(ModelReceiveAckReducer.class);
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {

    final Vector model = new DenseVector(dimensions);
    final long time1 = System.currentTimeMillis();
    final int numIters = 10;

    for (int i = 0; i < numIters; i++) {

      controlMessageBroadcaster.send(ControlMessages.ReceiveModel);
      modelBroadcaster.send(model);
      modelReceiveAckReducer.reduce();

      final GroupChanges changes = communicationGroupClient.getTopologyChanges();
      if (changes.exist()) {
        Log.info("There exist topology changes. Asking to update Topology");
        communicationGroupClient.updateTopology();
      } else {
        Log.info("No changes in topology exist. So not updating topology");
      }
    }

    final long time2 = System.currentTimeMillis();
    LOG.log(Level.FINE, "Broadcasting vector of dimensions {0} took {1} secs",
        new Object[]{dimensions, (time2 - time1) / (numIters * 1000.0)});

    controlMessageBroadcaster.send(ControlMessages.Stop);

    return null;
  }
}
