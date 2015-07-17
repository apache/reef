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
package org.apache.reef.io.network.shuffle.network;

import org.apache.reef.io.Tuple;

import java.util.List;

/**
 *
 */
public final class ShuffleTupleMessage<K, V> {

  private final String shuffleName;
  private final String groupingName;
  private final List<Tuple<K, V>> tuples;

  public ShuffleTupleMessage(
      final String shuffleName,
      final String groupingName,
      final List<Tuple<K, V>> tuples) {
    this.shuffleName = shuffleName;
    this.groupingName = groupingName;
    this.tuples = tuples;
  }

  public String getShuffleName() {
    return shuffleName;
  }

  public String getGroupingName() {
    return groupingName;
  }

  public int size() {
    if (tuples == null) {
      return 0;
    }

    return tuples.size();
  }

  public Tuple<K, V> get(final int index) {
    return tuples.get(index);
  }
}
