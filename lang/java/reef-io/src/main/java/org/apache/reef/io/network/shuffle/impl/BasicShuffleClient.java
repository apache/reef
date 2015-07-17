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
package org.apache.reef.io.network.shuffle.impl;

import org.apache.reef.io.network.shuffle.task.*;
import org.apache.reef.io.network.shuffle.description.ShuffleGroupDescription;
import org.apache.reef.io.network.shuffle.task.TupleReceiver;
import org.apache.reef.io.network.shuffle.task.TupleSender;

import javax.inject.Inject;

/**
 *
 */
public final class BasicShuffleClient implements ShuffleClient {

  private final ShuffleGroupDescription initialShuffleGroupDescription;
  private final TupleOperatorFactory operatorFactory;
  @Inject
  public BasicShuffleClient(
      final ShuffleGroupDescription initialShuffleGroupDescription,
      final TupleOperatorFactory operatorFactory) {
    this.initialShuffleGroupDescription = initialShuffleGroupDescription;
    this.operatorFactory = operatorFactory;
  }

  @Override
  public <K, V> TupleReceiver<K, V> getReceiver(final String shuffleName) {
    return operatorFactory.newTupleReceiver(initialShuffleGroupDescription.getShuffleDescription(shuffleName));
  }

  @Override
  public <K, V> TupleSender<K, V> getSender(final String shuffleName) {
    return operatorFactory.newTupleSender(initialShuffleGroupDescription.getShuffleDescription(shuffleName));
  }

  @Override
  public ShuffleGroupDescription getShuffleGroupDescription() {
    return initialShuffleGroupDescription;
  }
}
