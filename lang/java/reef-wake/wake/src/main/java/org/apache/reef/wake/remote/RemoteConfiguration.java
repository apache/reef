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

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.wake.EStage;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.WakeParameters;
import org.apache.reef.wake.remote.impl.DefaultTransportEStage;
import org.apache.reef.wake.remote.impl.ObjectSerializableCodec;
import org.apache.reef.wake.remote.impl.TransportEvent;
import org.apache.reef.wake.remote.transport.TransportFactory.ProtocolTypes;

/**
 * Configuration options and helper methods for Wake remoting.
 */
public final class RemoteConfiguration {

  /**
   * The number of tries to reconnect the remote connection.
   */
  public static final long REMOTE_CONNECTION_NUMBER_OF_RETRIES = 3;

  /**
   * The timeout of connection retrying.
   * To prevent retrying connections from being rejected by the remote stages,
   * the retrying_timeout * number_of_retries should be less than the remote_executor_shutdown_timeout.
   * If not, the remote stage can shutdown the connection retries before it is established,
   * and can drop a message that should be sent to the remote.
   */
  public static final long REMOTE_CONNECTION_RETRY_TIMEOUT =
      WakeParameters.REMOTE_EXECUTOR_SHUTDOWN_TIMEOUT / (REMOTE_CONNECTION_NUMBER_OF_RETRIES + 1);


  /**
   *  Unique protocol String for choosing protocols.
   */
  public static final String PROTOCOL_TCP = ProtocolTypes.TCP.name();
  public static final String PROTOCOL_HTTP = ProtocolTypes.HTTP.name();
  //TODO[JIRA REEF-1871] Implement HTTPS as protocol

  private RemoteConfiguration() {
    // empty
  }

  /**
   * The name of the remote manager.
   */
  @NamedParameter(short_name = "rm_name", doc = "The name of the remote manager.", default_value = "REEF_CLIENT")
  public static final class ManagerName implements Name<String> {
    // Intentionally empty
  }

  /**
   * The host address to be used for messages.
   */
  @NamedParameter(short_name = "rm_host", doc = "The host address to be used for messages.",
      default_value = "##UNKNOWN##")
  public static final class HostAddress implements Name<String> {
    // Intentionally empty
  }

  /**
   * The port to be used for messages.
   */
  @NamedParameter(short_name = "rm_port", doc = "The port to be used for messages.", default_value = "0")
  public static final class Port implements Name<Integer> {
    // Intentionally empty
  }

  /**
   * The codec to be used for messages.
   */
  @NamedParameter(doc = "The codec to be used for messages.", default_class = ObjectSerializableCodec.class)
  public static final class MessageCodec implements Name<Codec<?>> {
    // Intentionally empty
  }

  /**
   * The event handler to be used for throwables.
   */
  @NamedParameter(doc = "The event handler to be used for throwables.", default_class = DefaultErrorHandler.class)
  public static final class ErrorHandler implements Name<EventHandler<Throwable>> {
    // Intentionally empty
  }

  /**
   * Whether or not to use the message ordering guarantee.
   */
  @NamedParameter(short_name = "rm_order",
      doc = "Whether or not to use the message ordering guarantee.", default_value = "true")
  public static final class OrderingGuarantee implements Name<Boolean> {
    // Intentionally empty
  }

  /**
   * The number of tries.
   */
  @NamedParameter(doc = "The number of tries.", default_value = "" + REMOTE_CONNECTION_NUMBER_OF_RETRIES)
  public static final class NumberOfTries implements Name<Integer> {
    // Intentionally empty    
  }

  /**
   * The timeout of connection retrying.
   */
  @NamedParameter(doc = "The timeout of connection retrying.", default_value = "" + REMOTE_CONNECTION_RETRY_TIMEOUT)
  public static final class RetryTimeout implements Name<Integer> {
    // Intentionally empty       
  }

  /**
   * Client stage for messaging transport.
   */
  @NamedParameter(doc = "Client stage for messaging transport.", default_class = DefaultTransportEStage.class)
  public static final class RemoteClientStage implements Name<EStage<TransportEvent>> {
    // Intentionally empty
  }

  /**
   * Server stage for messaging transport.
   */
  @NamedParameter(doc = "Server stage for messaging transport.", default_class = DefaultTransportEStage.class)
  public static final class RemoteServerStage implements Name<EStage<TransportEvent>> {
    // Intentionally empty
  }

  /**
   * Option for use http.
   * Default value must be ProtocolTypes.TCP.name().
   */
  @NamedParameter(doc = "Option for use http.", default_value = "TCP")
  public static final class Protocol implements Name<String> {
    // Intentionally empty
  }
}
