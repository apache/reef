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
package org.apache.reef.io.network;

import org.apache.reef.io.network.impl.DefaultNetworkServiceImplementation;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.Stage;

import java.net.SocketAddress;
import java.util.List;

/**
 * Shared networking channel to transfer any type of event to an identifier
 * without a physical address. Note that the ordering of the events
 * is not guaranteed.
 */
@DefaultImplementation(DefaultNetworkServiceImplementation.class)
public interface NetworkService extends Stage {

  /**
   * returns network service's identifier
   *
   * @return local identifier
   */
  public Identifier getLocalId();

  /**
   * returns network service's socket address
   *
   * @return local socket address
   */
  public SocketAddress getLocalAddress();

  /**
   * returns naming proxy NetworkService collaborates with to register
   * it's identifier and look up remote address to send event.
   *
   * @return a naming proxy
   */
  public NamingProxy getNamingProxy();

  /**
   * Sends an event wrapped a list of which size is one
   *
   * @param remoteId
   * @param event
   * @param <T>
   */
  public <T> void sendEvent(Identifier remoteId, T event);

  /**
   * Sends a list of any event, already registered to an EventHandler in NetworkService,
   * to the remoteId specified in the parameter. If any type of Exception occurs,
   * the NetworkExceptionHandler's onException method will be called and notify
   * NetworkService of the error.
   *
   * Therefore, the event will either be successfully sent or a specified error
   * along with the sent event will be returned.
   *
   * Note that it synchronously sends event if connection is already established.
   * If there is no connection, it passes event to connecting queue and returns,
   * than that event will be sent in sender thread pool.
   *
   * @param remoteId
   * @param eventList
   * @param <T>
   */
  public <T> void sendEventList(Identifier remoteId, List<T> eventList);
}
