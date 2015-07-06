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

import org.apache.reef.examples.shuffle.params.WordCountTopology;
import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.shuffle.ns.ShuffleTupleMessage;
import org.apache.reef.io.network.shuffle.task.ShuffleService;
import org.apache.reef.io.network.shuffle.task.ShuffleTupleReceiver;
import org.apache.reef.task.Task;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;

/**
 *
 */
public final class AggregatorTask implements Task {

  @Inject
  public AggregatorTask(
      final ShuffleService shuffleService) {
    final ShuffleTupleReceiver<String, Integer> tupleReceiver = shuffleService.getTopologyClient(WordCountTopology.class)
        .getReceiver(WordCountDriver.AGGREGATING_GROUPING);
    tupleReceiver.registerMessageHandler(new MessageHandler());
  }

  @Override
  public byte[] call(byte[] memento) throws Exception {
    System.out.println("AggregatorTask");
    Thread.sleep(4000);
    return null;
  }

  private final class MessageHandler implements EventHandler<Message<ShuffleTupleMessage<String, Integer>>> {
    @Override
    public void onNext(Message<ShuffleTupleMessage<String, Integer>> msg) {
      System.out.println("message from " + msg.getSrcId());
      for (ShuffleTupleMessage<String, Integer> tupleMessage : msg.getData()) {
        for (int i = 0; i < tupleMessage.getDataLength(); i++) {
          System.out.println(tupleMessage.getDataAt(i));
        }
      }
    }
  }
}
