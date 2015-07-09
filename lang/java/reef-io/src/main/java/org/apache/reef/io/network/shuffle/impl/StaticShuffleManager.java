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
package org.apache.reef.io.network.shuffle.impl;

import org.apache.reef.driver.task.CompletedTask;
import org.apache.reef.driver.task.FailedTask;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.NetworkServiceClient;
import org.apache.reef.io.network.naming.NameServerParameters;
import org.apache.reef.io.network.shuffle.driver.ShuffleManager;
import org.apache.reef.io.network.shuffle.ns.ShuffleControlMessage;
import org.apache.reef.io.network.shuffle.params.ShuffleControlMessageNSId;
import org.apache.reef.io.network.shuffle.task.ShuffleClient;
import org.apache.reef.io.network.shuffle.descriptor.GroupingDescriptor;
import org.apache.reef.io.network.shuffle.descriptor.ShuffleDescriptor;
import org.apache.reef.io.network.shuffle.descriptor.ShuffleDescriptorSerializer;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.transport.LinkListener;

import javax.inject.Inject;
import java.net.SocketAddress;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 */
public final class StaticShuffleManager implements ShuffleManager {

  private static final Logger LOG = Logger.getLogger(StaticShuffleManager.class.getName());

  private final ShuffleDescriptor initialShuffleDescriptor;
  private final ShuffleDescriptorSerializer shuffleDescriptorSerializer;
  private final ShuffleLinkListener shuffleLinkListener;
  private final ShuffleMessageHandler shuffleMessageHandler;
  private final TaskEntityMap taskEntityMap;

  @Inject
  public StaticShuffleManager(
      final ShuffleDescriptor initialShuffleDescriptor,
      final ShuffleDescriptorSerializer shuffleDescriptorSerializer,
      final @Parameter(NameServerParameters.NameServerIdentifierFactory.class) IdentifierFactory idFactory,
      final NetworkServiceClient nsClient) {
    this.initialShuffleDescriptor = initialShuffleDescriptor;
    this.shuffleDescriptorSerializer = shuffleDescriptorSerializer;
    this.shuffleLinkListener = new ShuffleLinkListener();
    this.shuffleMessageHandler = new ShuffleMessageHandler();
    this.taskEntityMap = new TaskEntityMap(initialShuffleDescriptor, idFactory,
        nsClient.<ShuffleControlMessage>getConnectionFactory(ShuffleControlMessageNSId.class));

    createTaskEntityMap();
  }

  private void createTaskEntityMap() {
    for (final String groupingName : initialShuffleDescriptor.getGroupingNameList()) {
      final GroupingDescriptor descriptor = initialShuffleDescriptor.getGroupingDescriptor(groupingName);
      for (final String senderId : initialShuffleDescriptor.getSenderIdList(descriptor.getGroupingName())) {
        taskEntityMap.putTaskIdIfAbsent(senderId);
      }

      for (final String receiverId : initialShuffleDescriptor.getReceiverIdList(descriptor.getGroupingName())) {
        taskEntityMap.putTaskIdIfAbsent(receiverId);
      }
    }
  }

  @Override
  public EventHandler<Message<ShuffleControlMessage>> getControlMessageHandler() {
    return shuffleMessageHandler;
  }

  @Override
  public LinkListener<Message<ShuffleControlMessage>> getControlLinkListener() {
    return shuffleLinkListener;
  }

  @Override
  public ShuffleDescriptor getShuffleDescriptor() {
    return initialShuffleDescriptor;
  }

  @Override
  public Configuration getShuffleDescriptorConfigurationForTask(final String taskId) {
    return shuffleDescriptorSerializer.getConfigurationHasTaskId(initialShuffleDescriptor, taskId);
  }

  @Override
  public Class<? extends ShuffleClient> getClientClass() {
    return StaticShuffleClient.class;
  }

  @Override
  public void onRunningTask(final RunningTask runningTask) {
    taskEntityMap.onTaskStart(runningTask.getId());
  }

  @Override
  public void onFailedTask(final FailedTask failedTask) {
    taskEntityMap.onTaskStop(failedTask.getId());
  }

  @Override
  public void onCompletedTask(final CompletedTask completedTask) {
    taskEntityMap.onTaskStop(completedTask.getId());
  }

  private final class ShuffleLinkListener implements LinkListener<Message<ShuffleControlMessage>> {

    @Override
    public void onSuccess(final Message<ShuffleControlMessage> message) {
      LOG.log(Level.FINE, "A ShuffleMessage was successfully sent : {0}", message);
    }

    @Override
    public void onException(final Throwable cause, final SocketAddress remoteAddress, final Message<ShuffleControlMessage> message) {
      LOG.log(Level.FINE, "An exception occurred with a ShuffleMessage [{0}] caused by ", new Object[]{ message, cause });
      taskEntityMap.onTaskStop(message.getDestId().toString());
    }
  }

  private final class ShuffleMessageHandler implements EventHandler<Message<ShuffleControlMessage>> {

    @Override
    public void onNext(final Message<ShuffleControlMessage> message) {
      LOG.log(Level.FINE, "A ShuffleMessage was arrived {0}", message);
    }
  }
}
