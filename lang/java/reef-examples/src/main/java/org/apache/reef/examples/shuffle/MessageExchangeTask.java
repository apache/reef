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
package org.apache.reef.examples.shuffle;

import org.apache.reef.io.Tuple;
import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.shuffle.network.ShuffleTupleMessage;
import org.apache.reef.io.network.shuffle.task.ShuffleClient;
import org.apache.reef.io.network.shuffle.task.ShuffleService;
import org.apache.reef.io.network.shuffle.task.TupleReceiver;
import org.apache.reef.io.network.shuffle.task.TupleSender;
import org.apache.reef.task.Task;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.transport.LinkListener;

import javax.inject.Inject;
import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 */
public final class MessageExchangeTask implements Task {

  private static final Logger LOG = Logger.getLogger(MessageExchangeTask.class.getName());

  private final ShuffleClient shuffleClient;
  private final TupleSender<Integer, Integer> tupleSender;
  private final TupleReceiver<Integer, Integer> tupleReceiver;

  private final List<String> receiverList;
  private final int taskNumber;

  private final AtomicInteger counter;

  private final List<String> arrivedMessageSenderIdList;

  @Inject
  private MessageExchangeTask(
      final ShuffleService shuffleService) {
    this.shuffleClient = shuffleService.getClient(MessageExchangeDriver.MESSAGE_EXCHANGE_SHUFFLE_GROUP_NAME);
    this.tupleSender = shuffleClient.getSender(MessageExchangeDriver.MESSAGE_EXCHANGE_SHUFFLE_NAME);
    tupleSender.registerTupleLinkListener(new TupleLinkListener());
    this.tupleReceiver = shuffleClient.getReceiver(MessageExchangeDriver.MESSAGE_EXCHANGE_SHUFFLE_NAME);
    tupleReceiver.registerTupleMessageHandler(new TupleMessageHandler());
    this.receiverList = shuffleClient.getShuffleGroupDescription()
        .getReceiverIdList(MessageExchangeDriver.MESSAGE_EXCHANGE_SHUFFLE_NAME);
    this.taskNumber = receiverList.size();
    this.counter = new AtomicInteger(taskNumber);
    this.arrivedMessageSenderIdList = Collections.synchronizedList(new ArrayList<String>());
  }

  @Override
  public byte[] call(byte[] memento) throws Exception {
    // TODO: Currently MessageExchangeTasks are sleep 3 seconds to wait the other tasks start. I will add
    // synchronization logic for all tasks in same strategy via another pull request.
    Thread.sleep(3000);
    final List<String> messageSentIdList = tupleSender.sendTuple(generateRandomTuples());
    for (final String receiver : receiverList) {
      if (!messageSentIdList.contains(receiver)) {
        tupleSender.sendTupleTo(receiver, new ArrayList<Tuple<Integer, Integer>>());
      }
    }

    synchronized (counter) {
      while (counter.get() != 0) {
        counter.wait();
      }
    }

    return null;
  }

  private List<Tuple<Integer, Integer>> generateRandomTuples() {
    final Random rand = new Random();
    final List<Tuple<Integer, Integer>> randomTupleList = new ArrayList<>();
    for (int i = 0; i < taskNumber * 3 / 5; i++) {
      randomTupleList.add(new Tuple<>(rand.nextInt(), rand.nextInt()));
    }
    return randomTupleList;
  }

  private final class TupleLinkListener implements LinkListener<Message<ShuffleTupleMessage<Integer, Integer>>> {

    @Override
    public void onSuccess(final Message<ShuffleTupleMessage<Integer, Integer>> message) {
      LOG.log(Level.FINE, "{0} was successfully sent.", message);
    }

    @Override
    public void onException(
        final Throwable cause,
        final SocketAddress remoteAddress,
        final Message<ShuffleTupleMessage<Integer, Integer>> message) {
      throw new RuntimeException(cause);
    }
  }

  private final class TupleMessageHandler implements EventHandler<Message<ShuffleTupleMessage<Integer, Integer>>> {

    @Override
    public void onNext(final Message<ShuffleTupleMessage<Integer, Integer>> message) {
      if (arrivedMessageSenderIdList.contains(message.getSrcId().toString())) {
        throw new RuntimeException("Only one message from one node is allowed.");
      } else {
        if (counter.decrementAndGet() == 0) {
          LOG.log(Level.INFO, "{0} messages are arrived. The task will be notified and closed.", taskNumber);
          synchronized (counter) {
            counter.notifyAll();
          }
        }
      }

      for (final ShuffleTupleMessage<Integer, Integer> tupleMessage : message.getData()) {
        if (tupleMessage.size() == 0) {
          LOG.log(Level.INFO, "An empty shuffle message is arrived from {0}.", message.getSrcId());
        } else {
          LOG.log(Level.INFO, "A shuffle message with size {0} is arrived from {1}.",
              new Object[]{tupleMessage.size(), message.getSrcId()});
        }
        for (int i = 0; i < tupleMessage.size(); i++) {
          LOG.log(Level.INFO, tupleMessage.get(i).toString());
        }
      }
    }
  }
}
