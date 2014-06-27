/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.runtime.common.evaluator.context;

import com.microsoft.reef.runtime.common.evaluator.parameters.InitialTaskConfiguration;
import com.microsoft.reef.runtime.common.evaluator.parameters.RootContextConfiguration;
import com.microsoft.reef.runtime.common.evaluator.parameters.RootServiceConfiguration;
import com.microsoft.reef.util.Optional;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Injector;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.formats.ConfigurationSerializer;

import javax.inject.Inject;
import java.io.IOException;

/**
 * Helper class that encapsulates the root context configuration: With or without services and an initial task.
 */
final class RootContextLauncher {

  private ContextRuntime rootContext = null;

  private final Injector injector;

  private final Configuration rootContextConfiguration;

  private final Optional<Configuration> rootServiceConfiguration;

  private final Optional<Configuration> initialTaskConfiguration;

  private final ConfigurationSerializer configurationSerializer;

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
}
