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
import org.apache.reef.io.network.shuffle.network.ShuffleTupleMessage;
import org.apache.reef.io.network.shuffle.task.*;
import org.apache.reef.io.network.shuffle.task.operator.*;
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

  private ShuffleClient shuffleClient;
  private final SynchronizedTupleSender<String, Integer> tupleSender;
  private final SynchronizedTupleReceiver<String, Integer> tupleReceiver;
  private final Map<String, Integer> reduceMap;

  @Inject
  public ReducerTask(
      final ShuffleService shuffleService) {
    this.shuffleClient = shuffleService.getClient(WordCountShuffle.class);
    this.tupleSender = (SynchronizedTupleSender<String, Integer>) shuffleClient.<String, Integer>getSender(WordCountDriver.AGGREGATING_GROUPING);
    this.tupleReceiver = (SynchronizedTupleReceiver<String, Integer>) shuffleService.getClient(WordCountShuffle.class)
        .<String, Integer>getReceiver(WordCountDriver.SHUFFLE_GROUPING);
    this.reduceMap = new HashMap<>();
  }

  @Override
  public byte[] call(byte[] memento) throws Exception {
    System.out.println("ReducerTask");
    for (final Tuple<String, Integer> tuple : tupleReceiver.receiveTuples()) {
      addTuple(tuple);
    }

    List<Tuple<String, Integer>> reducedTupleList = new ArrayList<>();

    for (final Map.Entry<String, Integer> entry : reduceMap.entrySet()) {
      reducedTupleList.add(new Tuple<>(entry.getKey(), entry.getValue()));
    }

    tupleSender.sendTuple(reducedTupleList);
    return null;
  }

  private synchronized void addTuple(final Tuple<String, Integer> tuple) {
    if (!reduceMap.containsKey(tuple.getKey())) {
      reduceMap.put(tuple.getKey(), 0);
    }

    reduceMap.put(tuple.getKey(), tuple.getValue() + reduceMap.get(tuple.getKey()));
  }
}
