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
package org.apache.reef.io.network.shuffle.task;

import org.apache.reef.io.network.shuffle.ns.GlobalTupleCodecMap;
import org.apache.reef.io.network.shuffle.params.ShuffleKeyCodec;
import org.apache.reef.io.network.shuffle.params.ShuffleValueCodec;
import org.apache.reef.io.network.shuffle.description.GroupingDescription;
import org.apache.reef.io.network.shuffle.description.ShuffleDescription;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.remote.Codec;

import javax.inject.Inject;

/**
 *
 */
public final class ClientTupleCodecMapImpl implements ClientTupleCodecMap {

  private final String shuffleName;
  private final GlobalTupleCodecMap globalTupleCodecMap;

  @Inject
  public ClientTupleCodecMapImpl(
      final ShuffleDescription initialShuffleDescription,
      final GlobalTupleCodecMap globalTupleCodecMap) {

    this.globalTupleCodecMap = globalTupleCodecMap;
    shuffleName = initialShuffleDescription.getShuffleName().getName();
    for (final String groupingName : initialShuffleDescription.getGroupingNameList()) {
      registerTupleCodec(initialShuffleDescription.getGroupingDescription(groupingName));
    }
  }

  @Override
  public Codec<Tuple> getTupleCodec(final String groupingName) {
    return globalTupleCodecMap.getTupleCodec(shuffleName, groupingName);
  }

  @Override
  public void registerTupleCodec(final GroupingDescription groupingDescription) {
    final Injector injector = Tang.Factory.getTang().newInjector(
        Tang.Factory.getTang().newConfigurationBuilder()
            .bindNamedParameter(ShuffleKeyCodec.class, groupingDescription.getKeyCodec())
            .bindNamedParameter(ShuffleValueCodec.class, groupingDescription.getValueCodec())
            .build()
    );

    try {
      final Codec<Tuple> tupleCodec = injector.getInstance(TupleCodec.class);
      globalTupleCodecMap.registerTupleCodec(shuffleName, groupingDescription.getGroupingName(), tupleCodec);
    } catch (final InjectionException exception) {
      throw new RuntimeException("An InjectionException occurred while creating tuple codec for "
          + groupingDescription.getGroupingName(), exception);
    }
  }
}
