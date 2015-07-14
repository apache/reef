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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.reef.examples.shuffle.params.WordCountShuffle;
import org.apache.reef.io.data.loading.api.DataSet;
import org.apache.reef.io.network.shuffle.task.operator.TupleSender;
import org.apache.reef.io.network.shuffle.task.ShuffleService;
import org.apache.reef.io.network.shuffle.task.Tuple;
import org.apache.reef.io.network.util.Pair;
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

  private final DataSet<LongWritable, Text> dataSet;
  private final TupleSender<String, Integer> tupleSender;

  private final Map<String, Integer> reducedInputMap;

  @Inject
  public MapperTask(
      final ShuffleService shuffleService,
      final DataSet<LongWritable, Text> dataSet) {
    this.dataSet = dataSet;
    System.out.println(dataSet);
    this.tupleSender = shuffleService.getClient(WordCountShuffle.class).getSender(WordCountDriver.SHUFFLE_GROUPING);

    reducedInputMap = new HashMap<>();
  }

  @Override
  public byte[] call(byte[] memento) throws Exception {
    System.out.println("Mapper Task");
    // Thread.sleep(60000);
    createReducedInputMap();

    final List<Tuple<String, Integer>> tupleList = new ArrayList<>();

    for (final Map.Entry<String, Integer> entry : reducedInputMap.entrySet()) {
      final Tuple<String, Integer> tuple = new Tuple<>(entry.getKey(), entry.getValue());
      tupleList.add(tuple);
      System.out.println(tuple);
    }

    tupleSender.sendTuple(tupleList);
    return null;
  }

  private void createReducedInputMap() {
    for (final Pair<LongWritable, Text> pair : dataSet) {
      System.out.println(pair);
      final String[] words = pair.second.toString().replaceAll("  ", " ").split(" ");
      for (final String word : words) {
        if (!reducedInputMap.containsKey(word)) {
          reducedInputMap.put(word, 0);
        }

        reducedInputMap.put(word, 1 + reducedInputMap.get(word));
      }
    }
  }
}
