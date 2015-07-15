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

import org.apache.reef.evaluator.context.events.ContextStart;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.NetworkConnectionService;
import org.apache.reef.io.network.naming.NameServerParameters;
import org.apache.reef.io.network.shuffle.ns.*;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;

import javax.inject.Inject;

/**
 *
 */
public final class ShuffleContextStartHandler implements EventHandler<ContextStart> {

  private final NetworkConnectionService networkConnectionService;

  private final Identifier controlMessageConnectionId;
  private final ShuffleControlMessageCodec controlCodec;
  private final ShuffleControlMessageHandler controlHandler;
  private final ShuffleControlLinkListener controlLinkListener;

  private final Identifier tupleMessageConnectionId;
  private final ShuffleTupleMessageCodec tupleCodec;
  private final ShuffleTupleMessageHandler tupleHandler;
  private final ShuffleTupleLinkListener tupleLinkListener;

  @Inject
  public ShuffleContextStartHandler(
      final @Parameter(NameServerParameters.NameServerIdentifierFactory.class) IdentifierFactory idFactory,
      final NetworkConnectionService networkConnectionService,
      final ShuffleControlMessageCodec controlCodec,
      final ShuffleControlMessageHandler controlHandler,
      final ShuffleControlLinkListener controlLinkListener,
      final ShuffleTupleMessageCodec tupleCodec,
      final ShuffleTupleMessageHandler tupleHandler,
      final ShuffleTupleLinkListener tupleLinkListener) {
    this.networkConnectionService = networkConnectionService;
    this.controlMessageConnectionId = idFactory.getNewInstance(ShuffleNetworkConnectionId.CONTROL_MESSAGE);
    this.controlCodec = controlCodec;
    this.controlHandler = controlHandler;
    this.controlLinkListener = controlLinkListener;
    this.tupleMessageConnectionId = idFactory.getNewInstance(ShuffleNetworkConnectionId.TUPLE_MESSAGE);
    this.tupleCodec = tupleCodec;
    this.tupleHandler = tupleHandler;
    this.tupleLinkListener = tupleLinkListener;
  }

  @Override
  public void onNext(final ContextStart value) {
    try {
      networkConnectionService
          .registerConnectionFactory(controlMessageConnectionId, controlCodec, controlHandler, controlLinkListener);
      networkConnectionService
          .registerConnectionFactory(tupleMessageConnectionId, tupleCodec, tupleHandler, tupleLinkListener);
    } catch (NetworkException e) {
      throw new RuntimeException(e);
    }
  }
}
