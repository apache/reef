/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.io.network.nggroup.impl;

import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type;
import com.microsoft.tang.annotations.Name;
import com.microsoft.wake.EventHandler;

import javax.inject.Inject;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

/**
 *
 */
public class CommGroupNetworkHandlerImpl implements com.microsoft.reef.io.network.nggroup.api.CommGroupNetworkHandler {

  private static final Logger LOG = Logger.getLogger(CommGroupNetworkHandlerImpl.class.getName());

  private final Map<Class<? extends Name<String>>, EventHandler<GroupCommMessage>> operHandlers = new ConcurrentHashMap<>();
  private final Map<Class<? extends Name<String>>, BlockingQueue<GroupCommMessage>> topologyNotifications = new ConcurrentHashMap<>();

  @Inject
  public CommGroupNetworkHandlerImpl() {
  }


  @Override
  public void register(final Class<? extends Name<String>> operName,
                       final EventHandler<GroupCommMessage> operHandler) {
    operHandlers.put(operName, operHandler);
  }

  @Override
  public void addTopologyElement(final Class<? extends Name<String>> operName) {
    LOG.info("Creating LBQ for " + operName);
    topologyNotifications.put(operName, new LinkedBlockingQueue<GroupCommMessage>());
  }

  @Override
  public void onNext(final GroupCommMessage msg) {
    final Class<? extends Name<String>> operName = Utils.getClass(msg
        .getOperatorname());
    if (msg.getType() == Type.TopologyUpdated) {
      LOG.info("Got TopologyUpdate msg for " + operName + ". Adding to respective queue");
      topologyNotifications.get(operName).add(msg);
    } else if (msg.getType() == Type.TopologyChanges) {
      LOG.info("Got TopologyChanges msg for " + operName + ". Adding to respective queue");
      topologyNotifications.get(operName).add(msg);
    } else {
      operHandlers.get(operName).onNext(msg);
    }
  }

  @Override
  public byte[] waitForTopologyChanges(final Class<? extends Name<String>> operName) {
    try {
      LOG.info("Waiting for topology change msg for " + operName);
      return Utils.getData(topologyNotifications.get(operName).take());
    } catch (final InterruptedException e) {
      throw new RuntimeException("InterruptedException while waiting for topology update of " + operName.getSimpleName(), e);
    }
  }

  @Override
  public GroupCommMessage waitForTopologyUpdate(final Class<? extends Name<String>> operName) {
    try {
      LOG.info("Waiting for topology update msg for " + operName);
      return topologyNotifications.get(operName).take();
    } catch (final InterruptedException e) {
      throw new RuntimeException("InterruptedException while waiting for topology update of " + operName.getSimpleName(), e);
    }
  }

}
