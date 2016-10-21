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
import org.apache.reef.wake.Stage;
import org.apache.reef.wake.remote.impl.DefaultRemoteManagerImplementation;

/**
 * Represents all remote connections to and from one process to another.
 */
@DefaultImplementation(DefaultRemoteManagerImplementation.class)
public interface RemoteManager extends Stage {

  /**
   * Returns an event handler that can be used to send messages of type T to the
   * given destination.
   *
   * @param <T> type of message
   * @param destinationIdentifier a destination identifier
   * @param messageType           a message class type
   * @return an event handler
   */
  <T> EventHandler<T> getHandler(final RemoteIdentifier destinationIdentifier, final Class<? extends T> messageType);

  /**
   * Registers the given EventHandler to be invoked when messages of Type T
   * arrive from sourceIdentifier.
   * <p>
   * Calling this method twice overrides the initial registration.
   *
   * @param <T> type of event
   * @param <U> type of message
   * @param sourceIdentifier a source identifier
   * @param messageType      a message class type
   * @param theHandler       the event handler
   * @return the subscription that can be used to unsubscribe later
   */
  <T, U extends T> AutoCloseable registerHandler(final RemoteIdentifier sourceIdentifier,
                                                 final Class<U> messageType,
                                                 final EventHandler<T> theHandler);

  /**
   * Registers the given EventHandler to be called for the given message type
   * from any source.
   * <p>
   * If there is an EventHandler registered for this EventType
   *
   * @param <T> a type of remote message of event
   * @param <U> a type of message
   * @param messageType a message class type
   * @param theHandler  the event handler
   * @return the subscription that can be used to unsubscribe later
   */
  <T, U extends T> AutoCloseable registerHandler(final Class<U> messageType,
                                                 final EventHandler<RemoteMessage<T>> theHandler);

  /**
   * Access the Identifier of this.
   *
   * @return the Identifier of this.
   */
  RemoteIdentifier getMyIdentifier();
}
