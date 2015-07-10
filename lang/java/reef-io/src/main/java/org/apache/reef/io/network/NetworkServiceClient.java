/*
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
package org.apache.reef.io.network;

import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.impl.DefaultNetworkServiceClientImpl;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.remote.Codec;
import org.apache.reef.wake.remote.transport.LinkListener;

/**
 * NetworkServiceClient.
 *
 * NetworkServiceClient is a service which is designed for communicating messages with each other.
 * It creates multiple ConnectionFactories, which create multiple connections.
 *
 * Flow of message transfer:
 * [Downstream]: connection.write(message) -> ConnectionFactory -> Src NetworkServiceClient (encode) -> destination NetworkServiceClient.
 * [Upstream]: message -> Dest NetworkServiceClient (decode) -> ConnectionFactory -> EventHandler.
 *
 * Users can register a ConnectionFactory by registering their Codec, EventHandler and LinkListener.
 * When users send messages via connections created by the ConnectionFactory,
 * NetworkServiceClient encodes the messages according to the Codec registered in the ConnectionFactory and sends them to the destination NetworkServiceClient.
 * Also, it receives the messages by decoding the messages and forwarding them to the corresponding EventHandler registered in the ConnectionFactory.
 */
@DefaultImplementation(DefaultNetworkServiceClientImpl.class)
public interface NetworkServiceClient extends AutoCloseable {

  /**
   * Registers an instance of ConnectionFactory corresponding to the connectionFactoryId.
   * Binds Codec, EventHandler and LinkListener to the ConnectionFactory.
   * ConnectionFactory can create multiple connections between other NetworkServiceClients.
   *
   * @param connectionFactoryId a connection factory id
   * @param codec a codec for type <T>
   * @param eventHandler an event handler for type <T>
   * @param linkListener a link listener
   * @throws NetworkException throws a NetworkException when multiple connectionFactoryIds exist.
   */
  <T> void registerConnectionFactory(final Identifier connectionFactoryId,
                                     final Codec<T> codec,
                                     final EventHandler<Message<T>> eventHandler,
                                     final LinkListener<Message<T>> linkListener) throws NetworkException;

  /**
   * Unregisters a connectionFactory corresponding to the connectionFactoryId.
   * @param connectionFactoryId a connection factory id
   */
  void unregisterConnectionFactory(final Identifier connectionFactoryId);

  /**
   * Gets an instance of ConnectionFactory which is registered by the registerConnectionFactory method.
   * @param connectionFactoryId a connection factory id
   */
  <T> ConnectionFactory<T> getConnectionFactory(final Identifier connectionFactoryId);

  /**
   * Registers a network service client identifier.
   * This can be used for destination identifier
   * @param nsId network service client identifier
   */
  void registerId(final Identifier nsId);

  /**
   * Unregister a network service client identifier.
   * @param nsId network service client identifier
   */
  void unregisterId(final Identifier nsId);

  /**
   * Gets a network service client id which is equal to the registered id.
   */
  Identifier getNetworkServiceClientId();

}