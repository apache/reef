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

import org.apache.reef.io.network.naming.parameters.NameResolverCacheTimeout;
import org.apache.reef.io.network.naming.parameters.NameResolverRetryCount;
import org.apache.reef.io.network.naming.parameters.NameResolverRetryTimeout;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.OptionalParameter;

/**
 * Configuration Module Builder for LocalNameResolver.
 */
public final class LocalNameResolverConfiguration extends ConfigurationModuleBuilder {

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

  public static final ConfigurationModule CONF = new LocalNameResolverConfiguration()
      .bindNamedParameter(NameResolverCacheTimeout.class, CACHE_TIMEOUT)
      .bindNamedParameter(NameResolverRetryTimeout.class, RETRY_TIMEOUT)
      .bindNamedParameter(NameResolverRetryCount.class, RETRY_COUNT)
      .bindImplementation(NameResolver.class, LocalNameResolverImpl.class)
      .build();
}
