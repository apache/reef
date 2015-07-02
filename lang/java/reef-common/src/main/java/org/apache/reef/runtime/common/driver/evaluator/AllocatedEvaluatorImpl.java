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
package org.apache.reef.runtime.common.driver.evaluator;

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.CLRProcessFactory;
import org.apache.reef.driver.evaluator.EvaluatorDescriptor;
import org.apache.reef.driver.evaluator.EvaluatorType;
import org.apache.reef.driver.evaluator.EvaluatorProcess;
import org.apache.reef.driver.evaluator.JVMProcessFactory;
import org.apache.reef.runtime.common.driver.api.ResourceLaunchEventImpl;
import org.apache.reef.runtime.common.evaluator.EvaluatorConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.ConfigurationBuilder;
import org.apache.reef.tang.ConfigurationProvider;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.util.Optional;
import org.apache.reef.util.logging.LoggingScope;
import org.apache.reef.util.logging.LoggingScopeFactory;

import java.io.File;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Driver-Side representation of an allocated evaluator.
 */
@DriverSide
@Private
final class AllocatedEvaluatorImpl implements AllocatedEvaluator {

  private static final Logger LOG = Logger.getLogger(AllocatedEvaluatorImpl.class.getName());

  private final EvaluatorManager evaluatorManager;
  private final String remoteID;
  private final ConfigurationSerializer configurationSerializer;
  private final String jobIdentifier;
  private final LoggingScopeFactory loggingScopeFactory;
  private final Set<ConfigurationProvider> evaluatorConfigurationProviders;
  // TODO: The factories should be removed when deprecated setType is removed, as the process should not be created here
  private final JVMProcessFactory jvmProcessFactory;
  private final CLRProcessFactory clrProcessFactory;

  /**
   * The set of files to be places on the Evaluator.
   */
  private final Collection<File> files = new HashSet<>();
  /**
   * The set of libraries.
   */
  private final Collection<File> libraries = new HashSet<>();

  AllocatedEvaluatorImpl(final EvaluatorManager evaluatorManager,
                         final String remoteID,
                         final ConfigurationSerializer configurationSerializer,
                         final String jobIdentifier,
                         final LoggingScopeFactory loggingScopeFactory,
                         final Set<ConfigurationProvider> evaluatorConfigurationProviders,
                         final JVMProcessFactory jvmProcessFactory,
                         final CLRProcessFactory clrProcessFactory) {
    this.evaluatorManager = evaluatorManager;
    this.remoteID = remoteID;
    this.configurationSerializer = configurationSerializer;
    this.jobIdentifier = jobIdentifier;
    this.loggingScopeFactory = loggingScopeFactory;
    this.evaluatorConfigurationProviders = evaluatorConfigurationProviders;
    this.jvmProcessFactory = jvmProcessFactory;
    this.clrProcessFactory = clrProcessFactory;
  }

  @Override
  public String getId() {
    return this.evaluatorManager.getId();
  }

  @Override
  public void close() {
    this.evaluatorManager.close();
  }

  @Override
  public void submitTask(final Configuration taskConfiguration) {
    final Configuration contextConfiguration = ContextConfiguration.CONF
        .set(ContextConfiguration.IDENTIFIER, "RootContext_" + this.getId())
        .build();
    this.submitContextAndTask(contextConfiguration, taskConfiguration);

  }

  @Override
  public EvaluatorDescriptor getEvaluatorDescriptor() {
    return this.evaluatorManager.getEvaluatorDescriptor();
  }


  @Override
  public void submitContext(final Configuration contextConfiguration) {
    launch(contextConfiguration, Optional.<Configuration>empty(), Optional.<Configuration>empty());
  }

  @Override
  public void submitContextAndService(final Configuration contextConfiguration,
                                      final Configuration serviceConfiguration) {
    launch(contextConfiguration, Optional.of(serviceConfiguration), Optional.<Configuration>empty());
  }

  @Override
  public void submitContextAndTask(final Configuration contextConfiguration,
                                   final Configuration taskConfiguration) {
    launch(contextConfiguration, Optional.<Configuration>empty(), Optional.of(taskConfiguration));
  }

  @Override
  public void submitContextAndServiceAndTask(final Configuration contextConfiguration,
                                             final Configuration serviceConfiguration,
                                             final Configuration taskConfiguration) {
    launch(contextConfiguration, Optional.of(serviceConfiguration), Optional.of(taskConfiguration));
  }

  @Override
  @Deprecated
  public void setType(final EvaluatorType type) {
    switch (type) {
      case CLR:
        setProcess(clrProcessFactory.newEvaluatorProcess());
        break;
      default:
        setProcess(jvmProcessFactory.newEvaluatorProcess());
        break;
    }
  }

  @Override
  public void setProcess(final EvaluatorProcess process) {
    this.evaluatorManager.setProcess(process);
  }

  @Override
  public void addFile(final File file) {
    this.files.add(file);
  }

  @Override
  public void addLibrary(final File file) {
    this.libraries.add(file);
  }

  private void launch(final Configuration contextConfiguration,
                      final Optional<Configuration> serviceConfiguration,
                      final Optional<Configuration> taskConfiguration) {
    try (final LoggingScope lb = loggingScopeFactory.evaluatorLaunch(this.getId())) {
      final Configuration evaluatorConfiguration =
          makeEvaluatorConfiguration(contextConfiguration, serviceConfiguration, taskConfiguration);

      final ResourceLaunchEventImpl.Builder rbuilder =
          ResourceLaunchEventImpl.newBuilder()
              .setIdentifier(this.evaluatorManager.getId())
              .setRemoteId(this.remoteID)
              .setEvaluatorConf(evaluatorConfiguration)
              .addFiles(this.files)
              .addLibraries(this.libraries);

      rbuilder.setProcess(this.evaluatorManager.getEvaluatorDescriptor().getProcess());
      this.evaluatorManager.onResourceLaunch(rbuilder.build());
    }
  }

  private Configuration makeEvaluatorConfiguration(final Configuration contextConfiguration,
                                                   final Optional<Configuration> serviceConfiguration,
                                                   final Optional<Configuration> taskConfiguration) {

    final String contextConfigurationString = this.configurationSerializer.toString(contextConfiguration);
    ConfigurationModule evaluatorConfigurationModule = EvaluatorConfiguration.CONF
        .set(EvaluatorConfiguration.APPLICATION_IDENTIFIER, this.jobIdentifier)
        .set(EvaluatorConfiguration.DRIVER_REMOTE_IDENTIFIER, this.remoteID)
        .set(EvaluatorConfiguration.EVALUATOR_IDENTIFIER, this.getId())
        .set(EvaluatorConfiguration.ROOT_CONTEXT_CONFIGURATION, contextConfigurationString);

    // Add the (optional) service configuration
    final Optional<Configuration> mergedServiceConfiguration = makeRootServiceConfiguration(serviceConfiguration);
    if (mergedServiceConfiguration.isPresent()) {
      final String serviceConfigurationString = this.configurationSerializer.toString(mergedServiceConfiguration.get());
      evaluatorConfigurationModule = evaluatorConfigurationModule
          .set(EvaluatorConfiguration.ROOT_SERVICE_CONFIGURATION, serviceConfigurationString);
    }

    // Add the (optional) task configuration
    if (taskConfiguration.isPresent()) {
      final String taskConfigurationString = this.configurationSerializer.toString(taskConfiguration.get());
      evaluatorConfigurationModule = evaluatorConfigurationModule
          .set(EvaluatorConfiguration.TASK_CONFIGURATION, taskConfigurationString);
    }

    // Create the evaluator configuration.
    return evaluatorConfigurationModule.build();
  }

  /**
   * Merges the Configurations provided by the evaluatorConfigurationProviders into the given
   * serviceConfiguration, if any.
   */
  private Optional<Configuration> makeRootServiceConfiguration(final Optional<Configuration> serviceConfiguration) {
    final EvaluatorType evaluatorType = this.evaluatorManager.getEvaluatorDescriptor().getProcess().getType();
    if (EvaluatorType.CLR == evaluatorType) {
      LOG.log(Level.FINE, "Not using the ConfigurationProviders as we are configuring a {0} Evaluator.", evaluatorType);
      return serviceConfiguration;
    }

    if ((!serviceConfiguration.isPresent()) && this.evaluatorConfigurationProviders.isEmpty()) {
      // No configurations to merge.
      LOG.info("No service configuration given and no ConfigurationProviders set.");
      return Optional.empty();
    } else {
      final ConfigurationBuilder configurationBuilder = getConfigurationBuilder(serviceConfiguration);
      for (final ConfigurationProvider configurationProvider : this.evaluatorConfigurationProviders) {
        configurationBuilder.addConfiguration(configurationProvider.getConfiguration());
      }
      return Optional.of(configurationBuilder.build());
    }
  }

  /**
   * Utility to build a ConfigurationBuilder from an Optional<Configuration>.
   */
  private static ConfigurationBuilder getConfigurationBuilder(final Optional<Configuration> configuration) {
    if (configuration.isPresent()) {
      return Tang.Factory.getTang().newConfigurationBuilder(configuration.get());
    } else {
      return Tang.Factory.getTang().newConfigurationBuilder();
    }
  }

  @Override
  public String toString() {
    return "AllocatedEvaluator{ID='" + getId() + "\'}";
  }
}
