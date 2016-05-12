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

package org.apache.reef.io.network.naming;

import org.apache.reef.io.network.naming.parameters.*;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.OptionalParameter;
import org.apache.reef.tang.formats.RequiredParameter;
import org.apache.reef.wake.IdentifierFactory;

/**
 * Configuration Module Builder for NameResolver.
 */
public final class NameResolverConfiguration extends ConfigurationModuleBuilder {

  /**
   * The port used by name server.
   */
  public static final RequiredParameter<Integer> NAME_SERVICE_PORT = new RequiredParameter<>();
  /**
   * DNS hostname running the name service.
   */
  public static final RequiredParameter<String> NAME_SERVER_HOSTNAME = new RequiredParameter<>();

  /**
   * Identifier factory for NameClient.
   */
  public static final OptionalParameter<IdentifierFactory> IDENTIFIER_FACTORY = new OptionalParameter<>();

  /**
   * The timeout of caching lookup.
   */
  public static final OptionalParameter<Long> CACHE_TIMEOUT = new OptionalParameter<>();

  /**
   * The timeout of retrying connection.
   */
  public static final OptionalParameter<Integer> RETRY_TIMEOUT = new OptionalParameter<>();

  /**
   * The number of retrying connection.
   */
  public static final OptionalParameter<Integer> RETRY_COUNT = new OptionalParameter<>();

  public static final ConfigurationModule CONF = new NameResolverConfiguration()
      .bindNamedParameter(NameResolverNameServerPort.class, NAME_SERVICE_PORT)
      .bindNamedParameter(NameResolverNameServerAddr.class, NAME_SERVER_HOSTNAME)
      .bindNamedParameter(NameResolverIdentifierFactory.class, IDENTIFIER_FACTORY)
      .bindNamedParameter(NameResolverCacheTimeout.class, CACHE_TIMEOUT)
      .bindNamedParameter(NameResolverRetryTimeout.class, RETRY_TIMEOUT)
      .bindNamedParameter(NameResolverRetryCount.class, RETRY_COUNT)
      .build();
}
