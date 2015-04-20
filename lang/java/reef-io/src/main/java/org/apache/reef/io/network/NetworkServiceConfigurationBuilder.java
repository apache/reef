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
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.driver.task.TaskConfigurationOptions;
import org.apache.reef.evaluator.context.parameters.ContextStopHandlers;
import org.apache.reef.io.network.impl.NameClientProxy;
import org.apache.reef.io.network.impl.NetworkServiceBindTaskIdHandler;
import org.apache.reef.io.network.impl.NetworkServiceContextStopHandler;
import org.apache.reef.io.network.impl.NetworkServiceUnbindTaskIdHandler;
import org.apache.reef.io.network.naming.NameServerParameters;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.wake.remote.Codec;

import org.apache.reef.wake.remote.NetUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Builds a configuration for NetworkService to be used in an evaluator.
 */
@DriverSide
public final class NetworkServiceConfigurationBuilder {
  private final Set<Class<? extends NetworkEventHandler<?>>> handlers;
  private final Set<Class<? extends Codec<?>>> codecs;
  private final Set<Class<? extends NetworkLinkListener<?>>> linkListeners;
  private final NamingProxy namingProxy;
  private final List<String> eventClassNameList;
  private final String networkServiceId;
  private final Configuration conf;

  NetworkServiceConfigurationBuilder(
      final NamingProxy namingProxy,
      final List<String> eventClassNameList,
      final String networkServiceId,
      final Configuration conf) {
    this.namingProxy =  namingProxy;
    this.eventClassNameList = eventClassNameList;
    this.networkServiceId = networkServiceId;
    this.conf = conf;

    handlers = new HashSet<>();
    codecs = new HashSet<>();
    linkListeners = new HashSet<>();
  }

  /**
   * Adds a network event handler
   *
   * @param handler
   * @return this
   */
  public NetworkServiceConfigurationBuilder addEventHandler(final Class<? extends NetworkEventHandler<?>> handler) {
    handlers.add(handler);
    return this;
  }

  /**
   * Adds a codec
   *
   * @param codec
   * @return this
   */
  public NetworkServiceConfigurationBuilder addCodec(final Class<? extends Codec<?>> codec) {
    codecs.add(codec);
    return this;
  }

  /**
   * Adds a network link listener
   *
   * @param linkListener
   * @return this
   */
  public NetworkServiceConfigurationBuilder addLinkListener(final Class<? extends NetworkLinkListener<?>> linkListener) {
    linkListeners.add(linkListener);
    return this;
  }

  /**
   * Builds a configuration for NetworkService
   *
   * @return a configuration
   */
  public Configuration build() {
    final JavaConfigurationBuilder builder;
    if (conf == null) {
      builder = Tang.Factory.getTang().newConfigurationBuilder();
    } else {
      builder = Tang.Factory.getTang().newConfigurationBuilder(conf);
    }

    for (Class<? extends NetworkEventHandler<?>> handlerClass : handlers) {
      builder.bindSetEntry(NetworkServiceParameter.NetworkEventHandlers.class, handlerClass);
    }

    for (Class<? extends Codec<?>> codecClass : codecs) {
      builder.bindSetEntry(NetworkServiceParameter.Codecs.class, codecClass);
    }

    for (Class<? extends NetworkLinkListener<?>> linkListenerClass : linkListeners) {
      builder.bindSetEntry(NetworkServiceParameter.NetworkLinkListeners.class, linkListenerClass);
    }

    for (String eventClassName : eventClassNameList) {
      builder.bindSetEntry(NetworkServiceParameter.NetworkEvents.class, eventClassName);
    }

    if (networkServiceId != null) {
      builder.bindNamedParameter(NetworkServiceParameter.NetworkServiceIdentifier.class, networkServiceId);
    } else {
      builder
          .bindSetEntry(TaskConfigurationOptions.StartHandlers.class, NetworkServiceBindTaskIdHandler.class)
          .bindSetEntry(TaskConfigurationOptions.StopHandlers.class, NetworkServiceUnbindTaskIdHandler.class);
    }

   return builder
       .bindSetEntry(ContextStopHandlers.class, NetworkServiceContextStopHandler.class)
       .bindNamedParameter(NetworkServiceParameter.DriverNetworkServiceIdentifier.class, namingProxy.getLocalIdentifier().toString())
       .bindNamedParameter(NameServerParameters.NameServerPort.class, namingProxy.getNameServerPort() + "")
       .bindNamedParameter(NameServerParameters.NameServerAddr.class, NetUtils.getLocalAddress())
       .bindImplementation(NamingProxy.class, NameClientProxy.class)
       .build();
  }
}