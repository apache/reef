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

import org.apache.reef.io.network.naming.NameLookupClient;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.OptionalImpl;
import org.apache.reef.tang.formats.OptionalParameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.Codec;

/**
 * Network base configuration module
 */
public final class NetworkServiceBaseConfiguration extends ConfigurationModuleBuilder {

  /**
   *
   */
  public static final OptionalParameter<String> NETWORK_EVENTS = new OptionalParameter<>();


  /**
   * Event handler for network service. You have to set a directly implemented NetworkEventHandler
   * If there are two event handlers which have same hash code of event type, it can not register both of them.
   */
  public static final OptionalImpl<NetworkEventHandler<?>> NETWORK_EVENT_HANDLERS = new OptionalImpl<>();

  /**
   * Event codecs for network service. You have to set a directly implemented Codec
   * If there are two codecs which have same hash code of event type, it can not register both of them.
   */
  public static final OptionalImpl<Codec<?>> NETWORK_CODECS = new OptionalImpl<>();

  /**
   * Exception handlers for network service. You have to set a directly implemented NetworkLinkListener
   * If there are two link listeners which have same hash code of event type, it can not register both of them.
   */
  public static final OptionalImpl<NetworkLinkListener<?>> NETWORK_LINK_LISTENERS = new OptionalImpl<>();

  /**
   * Event handler for exception occurred.
   */
  public static final OptionalImpl<EventHandler<Throwable>> NETWORK_SERVICE_RECEIVE_ERROR_HANDLER = new OptionalImpl<>();

  /**
   * The number how many network service retries.
   */
  public static final OptionalParameter<Integer> NETWORK_SERVICE_RETRY_COUNT = new OptionalParameter<>();

  /**
   * The time when network service timeout. (msec)
   */
  public static final OptionalParameter<Integer> NETWORK_SERVICE_RETRY_TIME_OUT = new OptionalParameter<>();

  /**
   * The time when transport in naming proxy timeout. (msec)
   */
  public static final OptionalParameter<Long> NAME_LOOK_UP_REQUEST_TIME_OUT = new OptionalParameter<>();

  /**
   * The time when name look up client's cached entry timeout. (msec)
   */
  public static final OptionalParameter<Long> NAME_LOOK_UP_CACHE_TIME_OUT = new OptionalParameter<>();

  /**
   * The number how many name look up client retrying.
   */
  public static final OptionalParameter<Integer> NAME_LOOK_UP_RETRY_COUNT = new OptionalParameter<>();

  /**
   * The time when name look up client timeout. (msec)
   */
  public static final OptionalParameter<Integer> NAME_LOOK_UP_RETRY_TIME_OUT = new OptionalParameter<>();

  /**
   * If NetworkService's connecting queue size is over threshold, the thread who calls sendEvent method
   * waits for CONNECTING_QUEUE_WAITING_TIME
   */
  public static final OptionalParameter<Integer> CONNECTING_QUEUE_THRESHOLD = new OptionalParameter<>();

  /**
   * How much threads will be waiting. (msec)
   */
  public static final OptionalParameter<Integer> CONNECTING_QUEUE_WAITING_TIME = new OptionalParameter<>();

  /**
   * If you set this parameter false, all of network event from this network service does not know the sender's
   * identifier. You can reduce message's size if sender's id is not important information.
   */
  public static final OptionalParameter<Boolean> TRANSFER_SENDER_IDENTIFIER = new OptionalParameter<>();

  public static final ConfigurationModule CONF = new NetworkServiceBaseConfiguration()
      // NetworkService Parameters
      .bindSetEntry(NetworkServiceParameter.NetworkEvents.class, NETWORK_EVENTS)
      .bindSetEntry(NetworkServiceParameter.NetworkEventHandlers.class, NETWORK_EVENT_HANDLERS)
      .bindSetEntry(NetworkServiceParameter.Codecs.class, NETWORK_CODECS)
      .bindSetEntry(NetworkServiceParameter.NetworkLinkListeners.class, NETWORK_LINK_LISTENERS)
      .bindNamedParameter(NetworkServiceParameter.NetworkReceiveErrorHandler.class, NETWORK_SERVICE_RECEIVE_ERROR_HANDLER)
      .bindNamedParameter(NetworkServiceParameter.SenderRetryCount.class, NETWORK_SERVICE_RETRY_COUNT)
      .bindNamedParameter(NetworkServiceParameter.SenderRetryTimeout.class, NETWORK_SERVICE_RETRY_TIME_OUT)
      .bindNamedParameter(NetworkServiceParameter.ConnectingQueueSizeThreshold.class, CONNECTING_QUEUE_THRESHOLD)
      .bindNamedParameter(NetworkServiceParameter.ConnectingQueueWaitingTime.class, CONNECTING_QUEUE_WAITING_TIME)
      .bindNamedParameter(NetworkServiceParameter.TransferSenderIdentifier.class, TRANSFER_SENDER_IDENTIFIER)
      // NameLookupClient Parameters
      .bindNamedParameter(NameLookupClient.RequestTimeout.class, NAME_LOOK_UP_REQUEST_TIME_OUT)
      .bindNamedParameter(NameLookupClient.CacheTimeout.class, NAME_LOOK_UP_CACHE_TIME_OUT)
      .bindNamedParameter(NameLookupClient.RetryCount.class, NAME_LOOK_UP_RETRY_COUNT)
      .bindNamedParameter(NameLookupClient.RetryTimeout.class, NAME_LOOK_UP_RETRY_TIME_OUT)
      .build();
}
