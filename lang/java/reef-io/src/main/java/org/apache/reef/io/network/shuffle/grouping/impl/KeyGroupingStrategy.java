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
package org.apache.reef.io.network.shuffle.grouping.impl;

import org.apache.reef.io.network.shuffle.grouping.GroupingStrategy;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public final class KeyGroupingStrategy<K> implements GroupingStrategy<K> {

  @Inject
  public KeyGroupingStrategy() {
  }

  @Override
  public List<String> selectReceivers(final K key, final List<String> receiverIdList) {
    int index = key.hashCode() % receiverIdList.size();
    if (index < 0) {
      index += receiverIdList.size();
    }
    final List<String> list =  new ArrayList<>();
    list.add(receiverIdList.get(index));
    return list;
  }
}
