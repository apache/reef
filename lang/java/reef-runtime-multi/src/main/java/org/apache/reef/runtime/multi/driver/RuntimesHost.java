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
package org.apache.reef.runtime.multi.driver;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.reef.runtime.common.driver.api.*;
import org.apache.reef.runtime.multi.client.parameters.SerializedRuntimeDefinition;
import org.apache.reef.runtime.multi.driver.parameters.RuntimeName;
import org.apache.reef.runtime.multi.utils.MultiRuntimeDefinitionSerializer;
import org.apache.reef.runtime.multi.utils.avro.AvroMultiRuntimeDefinition;
import org.apache.reef.runtime.multi.utils.avro.AvroRuntimeDefinition;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.runtime.event.RuntimeStart;
import org.apache.reef.wake.time.runtime.event.RuntimeStop;
import org.apache.reef.webserver.HttpServer;

import javax.inject.Inject;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Hosts the actual runtime implementations and delegates invocations to them.
 */
final class RuntimesHost {

  private static final Logger LOG = Logger.getLogger(RuntimesHost.class.getName());

  private final AvroMultiRuntimeDefinition runtimeDefinition;
  private final Injector originalInjector;
  private final String defaultRuntimeName;

  private Map<String, Runtime> runtimes;

  @Inject
  private RuntimesHost(
      final Injector injector,
      @Parameter(SerializedRuntimeDefinition.class) final String serializedRuntimeDefinition) {

    this.originalInjector = injector;

    try {
      this.runtimeDefinition = new MultiRuntimeDefinitionSerializer().fromString(serializedRuntimeDefinition);
    } catch (final IOException e) {
      throw new RuntimeException("Unable to read runtime configuration.", e);
    }

    this.defaultRuntimeName = this.runtimeDefinition.getDefaultRuntimeName().toString();
  }

  /**
   * Initializes the configured runtimes.
   */
  private synchronized void initialize() {

    if (this.runtimes != null) {
      return;
    }

    this.runtimes = new HashMap<>();

    for (final AvroRuntimeDefinition rd : runtimeDefinition.getRuntimes()) {
      try {

        // We need to create different injector for each runtime as they define conflicting bindings. Also we cannot
        // fork the original injector because of the same reason.
        // We create new injectors and copy form the original injector what we need.
        // rootInjector is an emptyInjector that we copy bindings from the original injector into. Then we fork
        // it to instantiate the actual runtime.
        final Injector rootInjector = Tang.Factory.getTang().newInjector();
        initializeInjector(rootInjector);

        final Configuration runtimeConfig =
            Tang.Factory.getTang().newConfigurationBuilder()
                .bindNamedParameter(RuntimeName.class, rd.getRuntimeName().toString())
                .bindImplementation(Runtime.class, RuntimeImpl.class)
                .build();

        final Configuration config =
            new AvroConfigurationSerializer().fromString(rd.getSerializedConfiguration().toString());

        final Injector runtimeInjector = rootInjector.forkInjector(config, runtimeConfig);

        this.runtimes.put(rd.getRuntimeName().toString(), runtimeInjector.getInstance(Runtime.class));

      } catch (final IOException | InjectionException e) {
        throw new RuntimeException("Unable to initialize runtimes.", e);
      }
    }
  }

  /**
   * Copy event handler from current class configuration into runtime injector.
   * This is a helper method called from initializeInjector() only.
   * @param runtimeInjector Runtime injector to copy event handler to.
   * @param param Class that identifies the event handler parameter.
   * @param <T> Type of the event handler.
   * @throws InjectionException If configuration error occurs.
   */
  private <T extends EventHandler<?>> void copyEventHandler(
      final Injector runtimeInjector, final Class<? extends Name<T>> param) throws InjectionException {
    runtimeInjector.bindVolatileParameter(param, this.originalInjector.getNamedInstance(param));
  }

  /**
   * Initializes injector by copying needed handlers.
   * @param runtimeInjector The injector to initialize
   * @throws InjectionException on configuration error.
   */
  private void initializeInjector(final Injector runtimeInjector) throws InjectionException {

    copyEventHandler(runtimeInjector, RuntimeParameters.ResourceStatusHandler.class);
    copyEventHandler(runtimeInjector, RuntimeParameters.NodeDescriptorHandler.class);
    copyEventHandler(runtimeInjector, RuntimeParameters.ResourceAllocationHandler.class);
    copyEventHandler(runtimeInjector, RuntimeParameters.RuntimeStatusHandler.class);

    try {
      runtimeInjector.bindVolatileInstance(HttpServer.class, this.originalInjector.getInstance(HttpServer.class));
      LOG.log(Level.INFO, "Binding http server for the runtime implementation");
    } catch (final InjectionException e) {
      LOG.log(Level.INFO, "Http Server is not configured for the runtime", e);
    }
  }

  /**
   * Retrieves requested runtime, if requested name is empty a default runtime will be used.
   * @param requestedRuntimeName the requested runtime name
   * @return The runtime
   */
  private Runtime getRuntime(final String requestedRuntimeName) {

    final String runtimeName =
        StringUtils.isBlank(requestedRuntimeName) ? this.defaultRuntimeName : requestedRuntimeName;

    final Runtime runtime = this.runtimes.get(runtimeName);
    Validate.notNull(runtime, "Couldn't find runtime for name " + runtimeName);

    return runtime;
  }

  void onResourceLaunch(final ResourceLaunchEvent event) {
    getRuntime(event.getRuntimeName()).onResourceLaunch(event);
  }

  void onRuntimeStart(final RuntimeStart event) {
    initialize();
    for (final Runtime runtime : this.runtimes.values()) {
      runtime.onRuntimeStart(event);
    }
  }

  void onRuntimeStop(final RuntimeStop event) {
    for (final Runtime runtime : this.runtimes.values()) {
      runtime.onRuntimeStop(event);
    }
  }

  void onResourceRelease(final ResourceReleaseEvent event) {
    getRuntime(event.getRuntimeName()).onResourceRelease(event);
  }

  void onResourceRequest(final ResourceRequestEvent event) {
    getRuntime(event.getRuntimeName()).onResourceRequest(event);
  }
}
