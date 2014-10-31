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
package org.apache.reef.runtime.common.evaluator.context;

import org.apache.reef.runtime.common.evaluator.parameters.InitialTaskConfiguration;
import org.apache.reef.runtime.common.evaluator.parameters.RootContextConfiguration;
import org.apache.reef.runtime.common.evaluator.parameters.RootServiceConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.util.Optional;

import javax.inject.Inject;
import java.io.IOException;

/**
 * Helper class that encapsulates the root context configuration: With or without services and an initial task.
 */
final class RootContextLauncher {

  private final Injector injector;
  private final Configuration rootContextConfiguration;
  private final Optional<Configuration> rootServiceConfiguration;
  private final Optional<Configuration> initialTaskConfiguration;
  private final ConfigurationSerializer configurationSerializer;
  private ContextRuntime rootContext = null;

  @Inject
  RootContextLauncher(final @Parameter(RootContextConfiguration.class) String rootContextConfiguration,
                      final @Parameter(RootServiceConfiguration.class) String rootServiceConfiguration,
                      final @Parameter(InitialTaskConfiguration.class) String initialTaskConfiguration,
                      final Injector injector, final ConfigurationSerializer configurationSerializer) throws IOException, BindException {
    this.injector = injector;
    this.configurationSerializer = configurationSerializer;
    this.rootContextConfiguration = this.configurationSerializer.fromString(rootContextConfiguration);
    this.rootServiceConfiguration = Optional.of(this.configurationSerializer.fromString(rootServiceConfiguration));
    this.initialTaskConfiguration = Optional.of(this.configurationSerializer.fromString(initialTaskConfiguration));
  }

  @Inject
  RootContextLauncher(final @Parameter(RootContextConfiguration.class) String rootContextConfiguration,
                      final Injector injector,
                      final @Parameter(RootServiceConfiguration.class) String rootServiceConfiguration, final ConfigurationSerializer configurationSerializer) throws IOException, BindException {
    this.injector = injector;
    this.configurationSerializer = configurationSerializer;
    this.rootContextConfiguration = this.configurationSerializer.fromString(rootContextConfiguration);
    this.rootServiceConfiguration = Optional.of(this.configurationSerializer.fromString(rootServiceConfiguration));
    this.initialTaskConfiguration = Optional.empty();
  }

  @Inject
  RootContextLauncher(final Injector injector,
                      final @Parameter(RootContextConfiguration.class) String rootContextConfiguration,
                      final @Parameter(InitialTaskConfiguration.class) String initialTaskConfiguration, final ConfigurationSerializer configurationSerializer) throws IOException, BindException {
    this.injector = injector;
    this.configurationSerializer = configurationSerializer;
    this.rootContextConfiguration = this.configurationSerializer.fromString(rootContextConfiguration);
    this.rootServiceConfiguration = Optional.empty();
    this.initialTaskConfiguration = Optional.of(this.configurationSerializer.fromString(initialTaskConfiguration));
  }

  @Inject
  RootContextLauncher(final @Parameter(RootContextConfiguration.class) String rootContextConfiguration,
                      final Injector injector, final ConfigurationSerializer configurationSerializer) throws IOException, BindException {
    this.injector = injector;
    this.configurationSerializer = configurationSerializer;
    this.rootContextConfiguration = this.configurationSerializer.fromString(rootContextConfiguration);
    this.rootServiceConfiguration = Optional.empty();
    this.initialTaskConfiguration = Optional.empty();
  }

  /**
   * Instantiates the root context.
   * <p/>
   * This also launches the initial task if there is any.
   *
   * @param injector
   * @param rootContextConfiguration
   * @param rootServiceConfiguration
   * @return ContextRuntime
   * @throws ContextClientCodeException
   */
  private static ContextRuntime getRootContext(final Injector injector,
                                               final Configuration rootContextConfiguration,
                                               final Optional<Configuration> rootServiceConfiguration)
      throws ContextClientCodeException {
    final ContextRuntime result;
    if (rootServiceConfiguration.isPresent()) {
      final Injector rootServiceInjector;
      try {
        rootServiceInjector = injector.forkInjector(rootServiceConfiguration.get());
      } catch (final BindException e) {
        throw new ContextClientCodeException(ContextClientCodeException.getIdentifier(rootContextConfiguration),
            Optional.<String>empty(), "Unable to instatiate the root context", e);
      }
      result = new ContextRuntime(rootServiceInjector, rootContextConfiguration);
    } else {
      result = new ContextRuntime(injector.forkInjector(), rootContextConfiguration);
    }
    return result;
  }

  /**
   * @return the root context for this evaluator.
   */
  final ContextRuntime getRootContext() throws ContextClientCodeException {
    if (null == this.rootContext) {
      this.rootContext = getRootContext(this.injector, this.rootContextConfiguration, this.rootServiceConfiguration);
    }
    return this.rootContext;
  }

  final Optional<Configuration> getInitialTaskConfiguration() {
    return this.initialTaskConfiguration;
  }
}
