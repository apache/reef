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

import org.apache.reef.annotations.Unstable;
import org.apache.reef.io.network.impl.NetworkConnectionServiceImpl;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.remote.Codec;
import org.apache.reef.wake.remote.transport.LinkListener;

/**
 * NetworkConnectionService.
 * <p>
 * NetworkConnectionService is a service which is designed for communicating messages with each other.
 * It creates multiple ConnectionFactories, which create multiple connections.
 * <p>
 * Flow of message transfer:
 * <ul>
 * <li>[Downstream]: {@code connection.write(message) -> ConnectionFactory
 * -> src NetworkConnectionService (encode) -> dest NetworkConnectionService}.</li>
 * <li>[Upstream]: {@code message -> dest NetworkConnectionService (decode) -> ConnectionFactory -> EventHandler}.</li>
 * </ul>
 * <p>
 * Users can register a ConnectionFactory by registering their Codec, EventHandler, LinkListener and end point id.
 * When users send messages via connections created by the ConnectionFactory,
 * <p>
 * NetworkConnectionService encodes the messages according to the Codec
 * registered in the ConnectionFactory and sends them to the destination NetworkConnectionService.
 * <p>
 * Also, it receives the messages by decoding the messages and forwarding them
 * to the corresponding EventHandler registered in the ConnectionFactory.
 */
@Unstable
@DefaultImplementation(NetworkConnectionServiceImpl.class)
public interface NetworkConnectionService extends AutoCloseable {

  /**
   * Registers an instance of ConnectionFactory corresponding to the connectionFactoryId.
   * Binds Codec, EventHandler, LinkListener and localEndPointId to the ConnectionFactory.
   * ConnectionFactory can create multiple connections between other NetworkConnectionServices.
   * The connectionFactoryId is used to distinguish the type of connection and the localEndPointId
   * is the contact point, which is registered to NameServer through this method.
   *
   * @param connectionFactoryId a connection factory id
   * @param codec a codec for type T
   * @param eventHandler an event handler for type T
   * @param linkListener a link listener
   * @param localEndPointId a local end point id
   * @return the registered connection factory
   */
  <T> ConnectionFactory<T> registerConnectionFactory(Identifier connectionFactoryId,
                                     Codec<T> codec,
                                     EventHandler<Message<T>> eventHandler,
                                     LinkListener<Message<T>> linkListener,
                                     Identifier localEndPointId);

  /**
   * Unregisters a connectionFactory corresponding to the connectionFactoryId
   * and removes the localEndPointID of the connection factory from NameServer.
   * @param connectionFactoryId a connection factory id
   */
  void unregisterConnectionFactory(Identifier connectionFactoryId);

  /**
   * Gets an instance of ConnectionFactory which is registered by the registerConnectionFactory method.
   *
   * @param connectionFactoryId a connection factory id
   */
  <T> ConnectionFactory<T> getConnectionFactory(Identifier connectionFactoryId);

  /**
   * Closes all resources and unregisters all registered connection factories.
   *
   * @throws Exception if this resource cannot be closed
   */
  void close() throws Exception;

}
