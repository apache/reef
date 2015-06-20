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
import org.apache.reef.io.network.impl.DefaultNetworkServiceImpl;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.Stage;
import org.apache.reef.wake.remote.Codec;
import org.apache.reef.wake.remote.transport.LinkListener;

import java.net.SocketAddress;

@DefaultImplementation(DefaultNetworkServiceImpl.class)
/**
 * NetworkService for Task.
 * It can bind multiple ConnectionFactory.
 */
public interface NetworkService extends Stage {

  /**
   * Creates an instance of ConnectionFactory corresponding to the connectionFactoryId.
   * Binds Codec, EventHandler, and LinkListener to the ConnectionFactory.
   * @throws NetworkException throws a NetworkException when duplicated connectionFactoryId exists.
   */
  public <T> void registerConnectionFactory(final Class<? extends Name<String>> connectionFactoryId,
                                            final Codec<T> codec,
                                            final EventHandler<Message<T>> eventHandler,
                                            final LinkListener<Message<T>> linkListener) throws NetworkException;

  /**
   * Unregisters connectionFactory
   * @param connectionFactoryId
   */
  public void unregisterConnectionFactory(final Class<? extends Name<String>> connectionFactoryId);

  /**
   * Gets an instance of ConnectionFactory corresponding to the connectionFactoryId
   * @param connectionFactoryId the identifier of ConnectionFactory
   */
  public <T> ConnectionFactory<T> getConnectionFactory(final Class<? extends Name<String>> connectionFactoryId);

  /**
   * Register task identifier.
   * @param nsId
   */
  public void registerId(final Identifier nsId);

  /**
   * Unregister task identifier.
   * @param nsId
   */
  public void unregisterId(final Identifier nsId);

  /**
   * Get network service id which is equal to the registered id
   */
  public Identifier getNetworkServiceId();

  /**
   * Get local address
   */
  public SocketAddress getLocalAddress();
}
