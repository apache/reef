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

import org.apache.reef.io.network.impl.DefaultNetworkReceiveErrorHandler;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.Codec;

import java.util.Set;

/**
 * Configuration options and helper methods for network service.
 */
public final class NetworkServiceParameter {
  /**
   * Default driver network service id
   */
  public final static String DEFAULT_DRIVER_NETWORK_SERVICE_ID = "##UNKNOWN##DRIVER##ID";

  @NamedParameter(doc = "port for the network service", default_value = "0")
  public final static class NetworkServicePort implements Name<Integer> {
  }

  @NamedParameter(doc = "")
  public final static class NetworkEvents implements Name<Set<String>> {
  }

  @NamedParameter(doc = "network event handlers NetworkService use")
  public final static class NetworkEventHandlers implements Name<Set<NetworkEventHandler<?>>> {
  }

  @NamedParameter(doc = "network event codecs NetworkService use")
  public final static class Codecs implements Name<Set<Codec<?>>> {
  }

  @NamedParameter(doc = "network exception handlers NetworkService use")
  public final static class NetworkLinkListeners implements Name<Set<NetworkLinkListener<?>>> {
  }

  @NamedParameter(doc = "network exception handler when receiving events from remotes", default_class = DefaultNetworkReceiveErrorHandler.class)
  public final static class NetworkReceiveErrorHandler implements Name<EventHandler<Throwable>> {
  }

  @NamedParameter(doc = "NetworkService's id to communicate with each other.")
  public final static class NetworkServiceIdentifier implements Name<String> {
  }

  @NamedParameter(doc = "NetworkService Id of driver.", default_value = DEFAULT_DRIVER_NETWORK_SERVICE_ID)
  public final static class DriverNetworkServiceIdentifier implements Name<String> {
  }

  @NamedParameter(doc = "When should a retry timeout(msec)?", default_value = "1")
  public static class SenderRetryTimeout implements Name<Integer> {
  }

  @NamedParameter(doc = "How many times should NetworkService attempt to establish a connection?", default_value = "1")
  public static class SenderRetryCount implements Name<Integer> {
  }

  @NamedParameter(doc = "Whether the sender transfer event with it's identifier", default_value = "true")
  public static class TransferSenderIdentifier implements Name<Boolean> {
  }

  @NamedParameter(doc = "NetworkSender's connecting queue size threshold", default_value = "1000")
  public final static class ConnectingQueueSizeThreshold implements Name<Integer> {
  }

  @NamedParameter(doc = "NetworkSender's connecting queue waiting time(msec)", default_value = "100")
  public final static class ConnectingQueueWaitingTime implements Name<Integer> {
  }

  @NamedParameter(doc = "Codec for NetworkServiceEvent which enables remote nodes to encode/decode objects ", default_class = org.apache.reef.io.network.impl.NetworkServiceEventCodec.class)
  public final static class NetworkServiceEventCodec implements Name<Codec<?>> {
  }

  @NamedParameter(doc = "the number of thread in receiver thread pool", default_value = "10")
  public final static class NetworkReceiverThreadNumber implements Name<Integer> {
  }

  @NamedParameter(doc = "the number of thread in sender thread pool", default_value = "5")
  public final static class NetworkSenderThreadNumber implements Name<Integer> {
  }
}
