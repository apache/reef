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

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Factory to make NetworkServiceConfigurationBuilder which builds a configuration for
 * NetworkService to be used in an evaluator.
 */
@DriverSide
public final class NetworkServiceConfigurationBuilderFactory {
  private final List<String> sortedEventClassName;
  private final NetworkService networkService;

  @Inject
  public NetworkServiceConfigurationBuilderFactory(
      final NetworkService networkService,
      final @Parameter(NetworkServiceParameter.NetworkEvents.class) Set<String> networkEvents) {
    this.networkService = networkService;
    this.sortedEventClassName = new ArrayList<>(networkEvents.size());
    for (Object eventClassName : networkEvents.toArray()) {
      sortedEventClassName.add((String) eventClassName);
    }
  }

  /**
   * Returns a NetworkServiceConfigurationBuilder. The built configuration will be merged
   * with conf. If null is passed for id parameter, the network service in an evaluator bind
   * and unbind its task's id rather than has unique identifier.
   *
   * @param conf the built configuration will be merged with this conf
   * @param id identifier for new network service
   * @return a NetworkServiceConfigurationBuilder
   */
  public NetworkServiceConfigurationBuilder createBuilder(Configuration conf, String id) {
    return new NetworkServiceConfigurationBuilder(
        networkService.getNamingProxy(),
        sortedEventClassName,
        id,
        conf
    );
  }

  /**
   * Returns a NetworkServiceConfigurationBuilder.
   *
   * @param id identifier for new network service
   * @return a NetworkServiceConfigurationBuilder
   */
  public NetworkServiceConfigurationBuilder createBuilder(String id) {
    return createBuilder(null, id);
  }
}
