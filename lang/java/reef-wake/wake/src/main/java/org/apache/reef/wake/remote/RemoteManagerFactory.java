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
package org.apache.reef.wake.remote;

import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.remote.ports.TcpPortProvider;

/**
 * Injectable Factory for RemoteManager instances.
 * <p>
 * Use when direct injection of the RemoteManager is impossible.
 */
@DefaultImplementation(DefaultRemoteManagerFactory.class)
public interface RemoteManagerFactory {

  /**
   * @param name the name of used by the returned RemoteManager to determine the address to bind to. to instantiate.
   * @return a new instance of RemoteManager with all parameters but the given one injected via Tang.
   */
  RemoteManager getInstance(String name);

  /**
   * @param name         the name of the returned RemoteManager to instantiate.
   * @param codec        the codec to use to decode the messages sent to / by this RemoteManager.
   * @param errorHandler the error handler invoked for exceptions by the returned RemoteManager.
   * @param <T>          the message type sent / received by the returned RemoteManager.
   * @return a new instance of RemoteManager with all parameters but the given one injected via Tang.
   */
  <T> RemoteManager getInstance(String name,
                                Codec<T> codec,
                                EventHandler<Throwable> errorHandler);

  /**
   * @param name          the name of the returned RemoteManager to instantiate.
   * @param listeningPort the port on which the returned RemoteManager listens.
   * @param codec         the codec to use to decode the messages sent to / by this RemoteManager.
   * @param errorHandler  the error handler invoked for exceptions by the returned RemoteManager.
   * @param <T>           the message type sent / received by the returned RemoteManager.
   * @return a new instance of RemoteManager with all parameters but the given one injected via Tang.
   */
  <T> RemoteManager getInstance(String name,
                                int listeningPort,
                                Codec<T> codec,
                                EventHandler<Throwable> errorHandler);

  /**
   * @param name                 the name of the returned RemoteManager to instantiate.
   * @param hostAddress          the address the returned RemoteManager binds to.
   * @param listeningPort        the port on which the returned RemoteManager listens.
   * @param codec                the codec to use to decode the messages sent to / by this RemoteManager.
   * @param <T>                  the message type sent / received by the returned RemoteManager.
   * @return a new instance of RemoteManager with all parameters but the given one injected via Tang.
   */
  <T> RemoteManager getInstance(String name,
                                String hostAddress,
                                int listeningPort,
                                Codec<T> codec);

  /**
   * The old constructor of DefaultRemoteManagerImplementation. Avoid if you can.
   *
   * @param name              the name of the returned RemoteManager to instantiate.
   * @param hostAddress       the address the returned RemoteManager binds to.
   * @param listeningPort     the port on which the returned RemoteManager listens.
   * @param codec             the codec to use to decode the messages sent to / by this RemoteManager.
   * @param errorHandler      the error handler invoked for exceptions by the returned RemoteManager.
   * @param orderingGuarantee whether or not the returned RemoteManager should guarantee message orders.
   * @param numberOfTries     the number of retries before the returned RemoteManager declares sending a failure.
   * @param retryTimeout      the time (in ms) after which the returned RemoteManager considers a sending attempt
   *                          failed.
   * @param <T>               the message type sent / received by the returned RemoteManager.
   * @return a new instance of RemoteManager with all parameters but the given one injected via Tang.
   */
  <T> RemoteManager getInstance(String name,
                                String hostAddress,
                                int listeningPort,
                                Codec<T> codec,
                                EventHandler<Throwable> errorHandler,
                                boolean orderingGuarantee,
                                int numberOfTries,
                                int retryTimeout);

  /**
   * The all-out constructor of DefaultRemoteManagerImplementation. Avoid if you can.
   *
   * @param name                 the name of the returned RemoteManager to instantiate.
   * @param hostAddress          the address the returned RemoteManager binds to.
   * @param listeningPort        the port on which the returned RemoteManager listens.
   * @param codec                the codec to use to decode the messages sent to / by this RemoteManager.
   * @param errorHandler         the error handler invoked for exceptions by the returned RemoteManager.
   * @param orderingGuarantee    whether or not the returned RemoteManager should guarantee message orders.
   * @param numberOfTries        the number of retries before the returned RemoteManager declares sending a failure.
   * @param retryTimeout         the time (in ms) after which the returned RemoteManager considers a sending attempt
   *                             failed.
   * @param localAddressProvider the LocalAddressProvider used by the returned RemoteManager to determine the address
   *                             to bind to.
   * @param tcpPortProvider      the TcpPortProvider used by the returned RemoteManager to determine the port
   *                             to listen to.
   * @param <T>                  the message type sent / received by the returned RemoteManager.
   * @return a new instance of RemoteManager with all parameters but the given one injected via Tang.
   */
  <T> RemoteManager getInstance(String name,
                                String hostAddress,
                                int listeningPort,
                                Codec<T> codec,
                                EventHandler<Throwable> errorHandler,
                                boolean orderingGuarantee,
                                int numberOfTries,
                                int retryTimeout,
                                LocalAddressProvider localAddressProvider,
                                TcpPortProvider tcpPortProvider);


}
