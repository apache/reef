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
package com.microsoft.reef.runtime.common.driver;

import com.microsoft.reef.driver.catalog.NodeDescriptor;
import com.microsoft.reef.driver.evaluator.AllocatedEvaluator;
import com.microsoft.reef.driver.evaluator.EvaluatorType;
import com.microsoft.reef.proto.DriverRuntimeProtocol;
import com.microsoft.reef.proto.ReefServiceProtos;
import com.microsoft.reef.runtime.common.evaluator.EvaluatorConfigurationModule;
import com.microsoft.reef.util.Optional;
import com.microsoft.reef.util.TANGUtils;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.formats.ConfigurationFile;
import com.microsoft.tang.formats.ConfigurationModule;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

final class AllocatedEvaluatorImpl implements AllocatedEvaluator {

  private final EvaluatorManager evaluatorManager;
  private final String remoteID;

  /**
   * The set of files to be places on the Evaluator.
   */
  private final Set<File> files = new HashSet<>();
  /**
   * The set of libraries
   */
  private final Set<File> libraries = new HashSet<>();

  AllocatedEvaluatorImpl(final EvaluatorManager evaluatorManager, final String remoteID) {
    this.evaluatorManager = evaluatorManager;
    this.remoteID = remoteID;
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
  public final NodeDescriptor getNodeDescriptor() {
    return this.evaluatorManager.getNodeDescriptor();
  }

  @Override
  public final void submit(final Configuration contextConfiguration, final Configuration activityConfiguration) {
    this.submitContextAndActivity(contextConfiguration, activityConfiguration);
  }

  @Override
  public final void submit(final Configuration contextConfiguration) {
    submitContext(contextConfiguration);
  }

  @Override
  public void submitContext(final Configuration contextConfiguration) {
    launch(contextConfiguration, Optional.<Configuration>empty(), Optional.<Configuration>empty());
  }

  @Override
  public void submitContextAndService(final Configuration contextConfiguration, final Configuration serviceConfiguration) {
    launch(contextConfiguration, Optional.of(serviceConfiguration), Optional.<Configuration>empty());
  }

  @Override
  public void submitContextAndActivity(final Configuration contextConfiguration, final Configuration activityConfiguration) {
    launch(contextConfiguration, Optional.<Configuration>empty(), Optional.of(activityConfiguration));
  }

  @Override
  public void submitContextAndServiceAndActivity(final Configuration contextConfiguration,
                                                 final Configuration serviceConfiguration,
                                                 final Configuration activityConfiguration) {
    launch(contextConfiguration, Optional.of(serviceConfiguration), Optional.of(activityConfiguration));
  }

  @Override
  public void setType(final EvaluatorType type) {
    this.evaluatorManager.setType(type);
  }

  @Override
  public void addFileResource(final File file) throws IOException {
    if (file.getName().toLowerCase().endsWith(".jar")) {
      this.libraries.add(file);
    } else {
      this.files.add(file);
    }
  }

  @Override
  public void addFile(final File file) {
    this.files.add(file);
  }

  @Override
  public void addLibrary(final File file) {
    this.files.add(file);
  }

  private final void launch(final Configuration contextConfiguration,
                            final Optional<Configuration> serviceConfiguration,
                            final Optional<Configuration> activityConfiguration) {
    try {
      final ConfigurationModule evaluatorConfigurationModule = EvaluatorConfigurationModule.CONF
          .set(EvaluatorConfigurationModule.DRIVER_REMOTE_IDENTIFIER, this.remoteID)
          .set(EvaluatorConfigurationModule.EVALUATOR_IDENTIFIER, this.evaluatorManager.getId());

      final String encodedContextConfigurationString = TANGUtils.toStringEncoded(contextConfiguration);
      // Add the (optional) service configuration
      final ConfigurationModule contextConfigurationModule;
      if (serviceConfiguration.isPresent()) {
        // With service configuration
        final String encodedServiceConfigurationString = TANGUtils.toStringEncoded(serviceConfiguration.get());
        contextConfigurationModule = evaluatorConfigurationModule
            .set(EvaluatorConfigurationModule.ROOT_SERVICE_CONFIGURATION, encodedServiceConfigurationString)
            .set(EvaluatorConfigurationModule.ROOT_CONTEXT_CONFIGURATION, encodedContextConfigurationString);
      } else {
        // No service configuration
        contextConfigurationModule = evaluatorConfigurationModule
            .set(EvaluatorConfigurationModule.ROOT_CONTEXT_CONFIGURATION, encodedContextConfigurationString);
      }

      // Add the (optional) activity configuration
      final Configuration evaluatorConfiguration;
      if (activityConfiguration.isPresent()) {
        final String encodedActivityConfigurationString = TANGUtils.toStringEncoded(activityConfiguration.get());
        evaluatorConfiguration = contextConfigurationModule
            .set(EvaluatorConfigurationModule.ACTIVITY_CONFIGURATION, encodedActivityConfigurationString).build();
      } else {
        evaluatorConfiguration = contextConfigurationModule.build();
      }

      final DriverRuntimeProtocol.ResourceLaunchProto.Builder rbuilder = DriverRuntimeProtocol.ResourceLaunchProto.newBuilder()
          .setIdentifier(this.evaluatorManager.getId())
          .setRemoteId(this.remoteID)
          .setEvaluatorConf(ConfigurationFile.toConfigurationString(evaluatorConfiguration));

      for (final File file : this.files) {
        rbuilder.addFile(ReefServiceProtos.FileResourceProto.newBuilder().setName(file.getName()).setPath(file.getPath()).setType(ReefServiceProtos.FileType.PLAIN).build());
      }

      for (final File lib : this.libraries) {
        rbuilder.addFile(ReefServiceProtos.FileResourceProto.newBuilder().setName(lib.getName()).setPath(lib.getPath().toString()).setType(ReefServiceProtos.FileType.LIB).build());
      }

      { // Set the type
        switch (this.evaluatorManager.getType()) {
          case CLR:
            rbuilder.setType(ReefServiceProtos.ProcessType.CLR);
            break;
          case JVM:
            rbuilder.setType(ReefServiceProtos.ProcessType.JVM);
        }
      }

      this.evaluatorManager.handle(rbuilder.build());
    } catch (final BindException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String toString() {
    return "AllocatedEvaluator{" +
        "ID='" + getId() + '\'' +
        '}';
  }
}
