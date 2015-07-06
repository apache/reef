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

import org.apache.reef.examples.shuffle.params.InputString;
import org.apache.reef.examples.shuffle.params.WordCountTopology;
import org.apache.reef.io.network.shuffle.task.ShuffleTupleSender;
import org.apache.reef.io.network.shuffle.task.ShuffleService;
import org.apache.reef.io.network.shuffle.task.Tuple;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public final class MapperTask implements Task {

  private final ShuffleTupleSender<String, Integer> tupleSender;
  private final String input;
  @Inject
  public MapperTask(
      final ShuffleService shuffleService,
      final @Parameter(InputString.class) String input) {
    this.tupleSender = shuffleService.getTopologyClient(WordCountTopology.class).getSender(WordCountDriver.SHUFFLE_GROUPING);
    this.input = input;
  }

  @Override
  public byte[] call(byte[] memento) throws Exception {
    final Map<String, Integer> reducedInputMap = new HashMap<>();
    for (final String word : input.split(" ")) {
      if (!reducedInputMap.containsKey(word)) {
        reducedInputMap.put(word, 0);
      }

      reducedInputMap.put(word, 1 + reducedInputMap.get(word));
    }

    final List<Tuple<String, Integer>> tupleList = new ArrayList<>();

    for (final Map.Entry<String, Integer> entry : reducedInputMap.entrySet()) {
      final Tuple<String, Integer> tuple = new Tuple<>(entry.getKey(), entry.getValue());
      tupleList.add(tuple);
      System.out.println(tuple);
    }

    tupleSender.sendTuple(tupleList);
    return null;
  }
}
