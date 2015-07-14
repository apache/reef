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

import org.apache.reef.examples.shuffle.params.WordCountShuffle;
import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.shuffle.ns.ShuffleTupleMessage;
import org.apache.reef.io.network.shuffle.task.ShuffleClient;
import org.apache.reef.io.network.shuffle.task.ShuffleService;
import org.apache.reef.io.network.shuffle.task.operator.TupleReceiver;
import org.apache.reef.task.Task;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;

/**
 *
 */
public final class AggregatorTask implements Task {

  private ShuffleClient shuffleClient;

  @Inject
  public AggregatorTask(
      final ShuffleService shuffleService) {
    this.shuffleClient = shuffleService.getClient(WordCountShuffle.class);
    final TupleReceiver<String, Integer> tupleReceiver = shuffleClient
        .getReceiver(WordCountDriver.AGGREGATING_GROUPING);
    tupleReceiver.registerTupleMessageHandler(new MessageHandler());
  }

  @Override
  public byte[] call(byte[] memento) throws Exception {
    System.out.println("AggregatorTask");
    shuffleClient.waitForSetup();
    Thread.sleep(20000);
    // Thread.sleep(100000);
    return null;
  }

  private final class MessageHandler implements EventHandler<Message<ShuffleTupleMessage<String, Integer>>> {
    @Override
    public void onNext(Message<ShuffleTupleMessage<String, Integer>> msg) {
      System.out.println("message from " + msg.getSrcId());
      for (ShuffleTupleMessage<String, Integer> tupleMessage : msg.getData()) {
        for (int i = 0; i < tupleMessage.size(); i++) {
          System.out.println(tupleMessage.get(i));
        }
      }
    }
  }
}
