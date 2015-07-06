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
package org.apache.reef.io.network.shuffle.utils;

import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.Connection;
import org.apache.reef.io.network.NetworkServiceClient;
import org.apache.reef.io.network.naming.NameServerParameters;
import org.apache.reef.io.network.shuffle.driver.ShuffleDriverConfiguration;
import org.apache.reef.io.network.shuffle.ns.ShuffleControlMessage;
import org.apache.reef.io.network.shuffle.params.ShuffleControlMessageNSId;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.IdentifierFactory;

import javax.inject.Inject;

/**
 *
 */
public class ControlMessageSendHandler implements EventHandler<ShuffleControlMessage> {

  private Connection<ShuffleControlMessage> connection;

  @Inject
  private ControlMessageSendHandler(
      final NetworkServiceClient networkService,
      final @Parameter(NameServerParameters.NameServerIdentifierFactory.class) IdentifierFactory idFactory) {
    this.connection = networkService.<ShuffleControlMessage>getConnectionFactory(ShuffleControlMessageNSId.class)
        .newConnection(idFactory.getNewInstance(ShuffleDriverConfiguration.SHUFFLE_DRIVER_IDENTIFIER));
    try {
      this.connection.open();
    } catch (final NetworkException e) {
      throw new RuntimeException("Failed to connect with driver.", e);
    }
  }

  @Override
  public void onNext(final ShuffleControlMessage value) {
    connection.write(value);
  }
}
