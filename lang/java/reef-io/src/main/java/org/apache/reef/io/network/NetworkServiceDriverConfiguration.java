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

import org.apache.reef.evaluator.context.parameters.ContextStopHandlers;
import org.apache.reef.io.network.impl.NameServerProxy;
import org.apache.reef.io.network.impl.NetworkServiceContextStopHandler;
import org.apache.reef.io.network.naming.NameServerParameters;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.OptionalParameter;
import org.apache.reef.tang.formats.RequiredParameter;

/**
 * A configuration module for NetworkService on the driver
 */
public final class NetworkServiceDriverConfiguration extends ConfigurationModuleBuilder {

  /**
   * NetworkService's identifier. Remote nodes can send event using this identifier.
   */
  public static final RequiredParameter<String> NETWORK_SERVICE_ID = new RequiredParameter<>();

  /**
   * Port number of name server.
   */
  public static final OptionalParameter<Integer> NAME_SERVER_PORT = new OptionalParameter<>();

  public static final ConfigurationModule CONF = new NetworkServiceDriverConfiguration()
      .merge(NetworkServiceBaseConfiguration.CONF)
      .bindSetEntry(ContextStopHandlers.class, NetworkServiceContextStopHandler.class)
      .bindNamedParameter(NetworkServiceParameter.NetworkServiceIdentifier.class, NETWORK_SERVICE_ID)
      .bindNamedParameter(NetworkServiceParameter.DriverNetworkServiceIdentifier.class, NETWORK_SERVICE_ID)
      // NameServer Parameters
      .bindNamedParameter(NameServerParameters.NameServerPort.class, NAME_SERVER_PORT)
      // bind naming proxy
      .bindImplementation(NamingProxy.class, NameServerProxy.class)
      .build();
}
