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
import org.apache.reef.io.network.shuffle.task.ShuffleTupleSender;
import org.apache.reef.io.network.shuffle.task.Tuple;
import org.apache.reef.task.Task;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public final class ReducerTask implements Task {

  private final ShuffleTupleSender<String, Integer> tupleSender;
  private final Map<String, Integer> reduceMap;
  @Inject
  public ReducerTask(
      final ShuffleService shuffleService) {
    this.tupleSender = shuffleService.getTopologyClient(WordCountTopology.class).getSender(WordCountDriver.AGGREGATING_GROUPING);
    final ShuffleTupleReceiver<String, Integer> tupleReceiver = shuffleService.getTopologyClient(WordCountTopology.class)
        .getReceiver(WordCountDriver.SHUFFLE_GROUPING);
    tupleReceiver.registerMessageHandler(new MessageHandler());
    this.reduceMap = new HashMap<>();
  }

  @Override
  public byte[] call(byte[] memento) throws Exception {
    System.out.println("ReducerTask");
    Thread.sleep(2000);

    List<Tuple<String, Integer>> tupleList = new ArrayList<>();

    for (final Map.Entry<String, Integer> entry : reduceMap.entrySet()) {
      tupleList.add(new Tuple<>(entry.getKey(), entry.getValue()));
    }

    tupleSender.sendTuple(tupleList);

    return null;
  }

  private synchronized void addTuple(final Tuple<String, Integer> tuple) {
    if (!reduceMap.containsKey(tuple.getKey())) {
      reduceMap.put(tuple.getKey(), 0);
    }

    reduceMap.put(tuple.getKey(), tuple.getValue() + reduceMap.get(tuple.getKey()));
  }

  private final class MessageHandler implements EventHandler<Message<ShuffleTupleMessage<String, Integer>>> {
    @Override
    public void onNext(Message<ShuffleTupleMessage<String, Integer>> msg) {
      System.out.println("message from " + msg.getSrcId());
      for (ShuffleTupleMessage<String, Integer> tupleMessage : msg.getData()) {
        for (int i = 0; i < tupleMessage.getDataLength(); i++) {
          System.out.println(tupleMessage.getDataAt(i));
          addTuple(tupleMessage.getDataAt(i));
        }
      }
    }
  }
}
