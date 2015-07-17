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
package org.apache.reef.io.network.shuffle.task.operator;

import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.shuffle.GroupingController;
import org.apache.reef.io.network.shuffle.description.GroupingDescription;
import org.apache.reef.io.network.shuffle.network.ShuffleControlMessage;
import org.apache.reef.io.network.shuffle.network.ShuffleTupleMessage;
import org.apache.reef.io.network.shuffle.task.Tuple;
import org.apache.reef.wake.remote.transport.LinkListener;

import javax.inject.Inject;
import java.net.SocketAddress;
import java.util.List;

/**
 *
 */
public class SynchronizedTupleSender<K, V> implements TupleSender<K, V> {

  private final TupleSender<K, V> baseSender;

  @Inject
  public SynchronizedTupleSender(final TupleSender<K, V> baseSender) {
    this.baseSender = baseSender;
    baseSender.registerTupleLinkListener(new SenderLinkListener());
    baseSender.registerGroupingController(new SenderGroupingController());
  }

  @Override
  public int sendTuple(Tuple<K, V> tuple) {
    return baseSender.sendTuple(tuple);
  }

  @Override
  public int sendTuple(List<Tuple<K, V>> tupleList) {
    return baseSender.sendTuple(tupleList);
  }

  @Override
  public int sendTupleTo(String destNodeId, Tuple<K, V> tuple) {
    return baseSender.sendTupleTo(destNodeId, tuple);
  }

  @Override
  public int sendTupleTo(String destNodeId, List<Tuple<K, V>> tupleList) {
    return baseSender.sendTupleTo(destNodeId, tupleList);
  }

  @Override
  public void registerTupleLinkListener(LinkListener<Message<ShuffleTupleMessage<K, V>>> linkListener) {
    baseSender.registerTupleLinkListener(linkListener);
  }

  @Override
  public GroupingDescription<K, V> getGroupingDescription() {
    return baseSender.getGroupingDescription();
  }

  @Override
  public List<String> getSelectedReceiverIdList(K key) {
    return baseSender.getSelectedReceiverIdList(key);
  }

  @Override
  public void waitForGroupingSetup() {
    baseSender.waitForGroupingSetup();
  }

  @Override
  public void registerGroupingController(GroupingController groupingController) {
    baseSender.registerGroupingController(groupingController);
  }

  @Override
  public void sendControlMessage(String destId, int code, byte[][] data, byte type) {
    baseSender.sendControlMessage(destId, code, data, type);
  }

  @Override
  public void sendControlMessageToDriver(int code, byte[][] data, byte type) {
    baseSender.sendControlMessageToDriver(code, data, type);
  }

  private class SenderLinkListener implements LinkListener<Message<ShuffleTupleMessage<K, V>>> {

    @Override
    public void onSuccess(final Message<ShuffleTupleMessage<K, V>> message) {

    }

    @Override
    public void onException(
        final Throwable cause, final SocketAddress remoteAddress, final Message<ShuffleTupleMessage<K, V>> message) {

    }
  }

  private class SenderGroupingController implements GroupingController {

    @Override
    public GroupingDescription getGroupingDescription() {
      return baseSender.getGroupingDescription();
    }

    @Override
    public void onNext(Message<ShuffleControlMessage> value) {

    }

    @Override
    public void onSuccess(Message<ShuffleControlMessage> message) {

    }

    @Override
    public void onException(Throwable cause, SocketAddress remoteAddress, Message<ShuffleControlMessage> message) {

    }
  }
}
