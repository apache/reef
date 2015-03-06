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
import org.apache.reef.examples.group.broadcast.parameters.ModelBroadcaster;
import org.apache.reef.examples.group.broadcast.parameters.ModelReceiveAckReducer;
import org.apache.reef.examples.group.utils.math.Vector;
import org.apache.reef.io.network.group.api.operators.Broadcast;
import org.apache.reef.io.network.group.api.operators.Reduce;
import org.apache.reef.io.network.group.api.task.CommunicationGroupClient;
import org.apache.reef.io.network.group.api.task.GroupCommClient;
import org.apache.reef.task.Task;

import javax.inject.Inject;

/**
 *
 */
public class SlaveTask implements Task {
  private final CommunicationGroupClient communicationGroupClient;
  private final Broadcast.Receiver<ControlMessages> controlMessageBroadcaster;
  private final Broadcast.Receiver<Vector> modelBroadcaster;
  private final Reduce.Sender<Boolean> modelReceiveAckReducer;

  @Inject
  public SlaveTask(
      final GroupCommClient groupCommClient) {
    this.communicationGroupClient = groupCommClient.getCommunicationGroup(AllCommunicationGroup.class);
    this.controlMessageBroadcaster = communicationGroupClient.getBroadcastReceiver(ControlMessageBroadcaster.class);
    this.modelBroadcaster = communicationGroupClient.getBroadcastReceiver(ModelBroadcaster.class);
    this.modelReceiveAckReducer = communicationGroupClient.getReduceSender(ModelReceiveAckReducer.class);
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {
    boolean stop = false;
    while (!stop) {
      final ControlMessages controlMessage = controlMessageBroadcaster.receive();
      switch (controlMessage) {
        case Stop:
          stop = true;
          break;

        case ReceiveModel:
          modelBroadcaster.receive();
          if (Math.random() < 0.1) {
            throw new RuntimeException("Simulated Failure");
          }
          modelReceiveAckReducer.send(true);
          break;

        default:
          break;
      }
    }
    return null;
  }
}
