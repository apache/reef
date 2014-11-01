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
package org.apache.reef.io.network.impl;

import org.apache.reef.io.network.TransportFactory;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.Codec;

public class NetworkServiceParameters {

  @NamedParameter
  public static class TaskId implements Name<String> {

  }

  @NamedParameter(doc = "identifier factory for the service", short_name = "factory", default_class = StringIdentifierFactory.class)
  public static class NetworkServiceIdentifierFactory implements Name<IdentifierFactory> {
  }

  @NamedParameter(doc = "port for the network service", short_name = "nsport", default_value = "7070")
  public static class NetworkServicePort implements Name<Integer> {
  }

  @NamedParameter(doc = "codec for the network service", short_name = "nscodec")
  public static class NetworkServiceCodec implements Name<Codec<?>> {
  }

  @NamedParameter(doc = "transport factory for the network service", short_name = "nstransportfactory", default_class = MessagingTransportFactory.class)
  public static class NetworkServiceTransportFactory implements Name<TransportFactory> {
  }

  @NamedParameter(doc = "network receive handler for the network service", short_name = "nshandler")
  public static class NetworkServiceHandler implements Name<EventHandler<?>> {
  }

  @NamedParameter(doc = "network exception handler for the network service", short_name = "exhandler")
  public static class NetworkServiceExceptionHandler implements Name<EventHandler<?>> {
  }

}
