/*
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
package org.apache.reef.io.network.group.impl.task;

import org.apache.reef.io.network.group.api.task.CommGroupNetworkHandler;
import org.apache.reef.io.network.group.impl.GroupCommunicationMessage;
import org.apache.reef.io.network.group.impl.utils.Utils;
import org.apache.reef.io.network.proto.ReefNetworkGroupCommProtos;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

public class CommGroupNetworkHandlerImpl implements
    CommGroupNetworkHandler {

  private static final Logger LOG = Logger.getLogger(CommGroupNetworkHandlerImpl.class.getName());

  private final Map<Class<? extends Name<String>>, EventHandler<GroupCommunicationMessage>> operHandlers =
      new ConcurrentHashMap<>();
  private final Map<Class<? extends Name<String>>, BlockingQueue<GroupCommunicationMessage>> topologyNotifications =
      new ConcurrentHashMap<>();

  @Inject
  public CommGroupNetworkHandlerImpl() {
  }

  @Override
  public void register(final Class<? extends Name<String>> operName,
                       final EventHandler<GroupCommunicationMessage> operHandler) {
    LOG.entering("CommGroupNetworkHandlerImpl", "register", new Object[]{Utils.simpleName(operName), operHandler});
    operHandlers.put(operName, operHandler);
    LOG.exiting("CommGroupNetworkHandlerImpl", "register",
        Arrays.toString(new Object[]{Utils.simpleName(operName), operHandler}));
  }

  @Override
  public void addTopologyElement(final Class<? extends Name<String>> operName) {
    LOG.entering("CommGroupNetworkHandlerImpl", "addTopologyElement", Utils.simpleName(operName));
    LOG.finest("Creating LBQ for " + operName);
    topologyNotifications.put(operName, new LinkedBlockingQueue<GroupCommunicationMessage>());
    LOG.exiting("CommGroupNetworkHandlerImpl", "addTopologyElement", Utils.simpleName(operName));
  }

  @Override
  public void onNext(final GroupCommunicationMessage msg) {
    LOG.entering("CommGroupNetworkHandlerImpl", "onNext", msg);
    final Class<? extends Name<String>> operName = Utils.getClass(msg.getOperatorname());
    if (msg.getType() == ReefNetworkGroupCommProtos.GroupCommMessage.Type.TopologyUpdated ||
        msg.getType() == ReefNetworkGroupCommProtos.GroupCommMessage.Type.TopologyChanges) {
      topologyNotifications.get(operName).add(msg);
    } else {
      operHandlers.get(operName).onNext(msg);
    }
    LOG.exiting("CommGroupNetworkHandlerImpl", "onNext", msg);
  }

  @Override
  public byte[] waitForTopologyChanges(final Class<? extends Name<String>> operName) {
    LOG.entering("CommGroupNetworkHandlerImpl", "waitForTopologyChanges", Utils.simpleName(operName));
    try {
      final byte[] retVal = Utils.getData(topologyNotifications.get(operName).take());
      LOG.exiting("CommGroupNetworkHandlerImpl", "waitForTopologyChanges", retVal);
      return retVal;
    } catch (final InterruptedException e) {
      throw new RuntimeException("InterruptedException while waiting for topology update of "
          + operName.getSimpleName(), e);
    }
  }

  @Override
  public GroupCommunicationMessage waitForTopologyUpdate(final Class<? extends Name<String>> operName) {
    LOG.entering("CommGroupNetworkHandlerImpl", "waitForTopologyUpdate", Utils.simpleName(operName));
    try {
      final GroupCommunicationMessage retVal = topologyNotifications.get(operName).take();
      LOG.exiting("CommGroupNetworkHandlerImpl", "waitForTopologyUpdate", retVal);
      return retVal;
    } catch (final InterruptedException e) {
      throw new RuntimeException("InterruptedException while waiting for topology update of "
          + operName.getSimpleName(), e);
    }
  }

}
