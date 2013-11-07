/**
 * Copyright (C) 2013 Microsoft Corporation
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

import com.microsoft.reef.runtime.common.evaluator.EvaluatorConfigurationModule;
import com.microsoft.reef.util.Optional;
import com.microsoft.reef.util.TANGUtils;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Injector;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.exceptions.BindException;

import javax.inject.Inject;

/**
 * Helper class that encapsulates the root context configuration: With or without services and an initial activity.
 */
final class RootContextLauncher {

  private ContextRuntime rootContext = null;

  private final Injector injector;

  private final Configuration rootContextConfiguration;

  private final Optional<Configuration> rootServiceConfiguration;

  private final Optional<Configuration> initialActivityConfiguration;

  @Inject
  RootContextLauncher(final @Parameter(EvaluatorConfigurationModule.RootContextConfiguration.class) String rootContextConfiguration,
                      final @Parameter(EvaluatorConfigurationModule.RootServiceConfiguration.class) String rootServiceConfiguration,
                      final @Parameter(EvaluatorConfigurationModule.ActivityConfiguration.class) String initialActivityConfiguration,
                      final Injector injector) {
    this.injector = injector;
    this.rootContextConfiguration = TANGUtils.fromStringEncoded(rootContextConfiguration);
    this.rootServiceConfiguration = Optional.of(TANGUtils.fromStringEncoded(rootServiceConfiguration));
    this.initialActivityConfiguration = Optional.of(TANGUtils.fromStringEncoded(initialActivityConfiguration));
  }

  @Inject
  RootContextLauncher(final @Parameter(EvaluatorConfigurationModule.RootContextConfiguration.class) String rootContextConfiguration,
                      final Injector injector,
                      final @Parameter(EvaluatorConfigurationModule.RootServiceConfiguration.class) String rootServiceConfiguration) {
    this.injector = injector;
    this.rootContextConfiguration = TANGUtils.fromStringEncoded(rootContextConfiguration);
    this.rootServiceConfiguration = Optional.of(TANGUtils.fromStringEncoded(rootServiceConfiguration));
    this.initialActivityConfiguration = Optional.empty();
  }

  @Inject
  RootContextLauncher(final Injector injector,
                      final @Parameter(EvaluatorConfigurationModule.RootContextConfiguration.class) String rootContextConfiguration,
                      final @Parameter(EvaluatorConfigurationModule.ActivityConfiguration.class) String initialActivityConfiguration) {
    this.injector = injector;
    this.rootContextConfiguration = TANGUtils.fromStringEncoded(rootContextConfiguration);
    this.rootServiceConfiguration = Optional.empty();
    this.initialActivityConfiguration = Optional.of(TANGUtils.fromStringEncoded(initialActivityConfiguration));
  }

  @Inject
  RootContextLauncher(final @Parameter(EvaluatorConfigurationModule.RootContextConfiguration.class) String rootContextConfiguration,
                      final Injector injector) {
    this.injector = injector;
    this.rootContextConfiguration = TANGUtils.fromStringEncoded(rootContextConfiguration);
    this.rootServiceConfiguration = Optional.empty();
    this.initialActivityConfiguration = Optional.empty();
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

  final Optional<Configuration> getInitialActivityConfiguration() {
    return this.initialActivityConfiguration;
  }

  /**
   * Instantiates the root context.
   * <p/>
   * This also launches the initial activity if there is any.
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
