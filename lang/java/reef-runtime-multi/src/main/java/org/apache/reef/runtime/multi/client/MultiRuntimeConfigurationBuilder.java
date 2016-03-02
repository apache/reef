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
package org.apache.reef.runtime.multi.client;

import org.apache.commons.lang.Validate;
import org.apache.reef.runtime.local.client.parameters.MaxNumberOfEvaluators;
import org.apache.reef.runtime.multi.client.parameters.DefaultRuntimeName;
import org.apache.reef.runtime.multi.client.parameters.RuntimeNames;
import org.apache.reef.runtime.yarn.client.ExtensibleYarnClientConfiguration;
import org.apache.reef.runtime.yarn.driver.RuntimeIdentifier;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.util.Optional;

import java.util.*;

/**
 * A builder for Multi Runtime Configuration.
 */
public final class MultiRuntimeConfigurationBuilder {
  private static final Set<String> SUPPORTED_RUNTIMES = new HashSet<>(Arrays.asList(
          org.apache.reef.runtime.yarn.driver.RuntimeIdentifier.RUNTIME_NAME,
          org.apache.reef.runtime.local.driver.RuntimeIdentifier.RUNTIME_NAME));
  private static final Set<String> SUPPORTED_SUBMISSION_RUNTIMES = new HashSet<>(Arrays.asList(
          org.apache.reef.runtime.yarn.driver.RuntimeIdentifier.RUNTIME_NAME));

  private final HashMap<Class, Object> namedParameters = new HashMap<>();

  private Set<String> runtimeNames = new HashSet<>();
  private Optional<String> defaultRuntime = null;
  private String submissionRunitme;

  private void addNamedParameter(final Class namedParameter,
                                 final Object namedParameterValue) {
    Validate.notNull(namedParameterValue);

    this.namedParameters.put(namedParameter, namedParameterValue);
  }

  /**
   * Adds runtime name to the builder.
   * @param runtimeName The name to add
   * @return The builder instance
   */
  public MultiRuntimeConfigurationBuilder addRuntime(final String runtimeName) {
    Validate.isTrue(SUPPORTED_RUNTIMES.contains(runtimeName), "unsupported runtime " + runtimeName);

    this.runtimeNames.add(runtimeName);
    return this;
  }

  /**
   * Sets default runtime. Default runtime is used when no runtime was specified for evaluator
   * @param runtimeName the default runtime name
   * @return The builder instance
   */
  public MultiRuntimeConfigurationBuilder setDefaultRuntime(final String runtimeName) {
    Validate.isTrue(SUPPORTED_RUNTIMES.contains(runtimeName), "Unsupported runtime " + runtimeName);
    Validate.isTrue(this.defaultRuntime == null, "Default runtime was already added");

    this.defaultRuntime = Optional.of(runtimeName);
    return this;
  }

  /**
   * Sets the submission runtime. Submission runtime is used for launching the job driver.
   * @param runtimeName the submission runtime name
   * @return The builder instance
   */
  public MultiRuntimeConfigurationBuilder setSubmissionRuntime(final String runtimeName) {
    Validate.isTrue(SUPPORTED_SUBMISSION_RUNTIMES.contains(runtimeName), "Unsupported submission runtime " +
            runtimeName);
    Validate.isTrue(this.submissionRunitme == null, "Submission runtime was already added");

    this.submissionRunitme = runtimeName;
    return this;
  }

  /**
   * Sets the max number of local evaluators for local runtime. This parameter is ignored when local runtime is not used
   * @param maxLocalEvaluators The max evaluators number
   * @return The builder instance
   */
  public MultiRuntimeConfigurationBuilder setMaxEvaluatorsNumberForLocalRuntime(final int maxLocalEvaluators) {
    Validate.isTrue(maxLocalEvaluators > 0, "Max evaluators number should be greater then 0");

    addNamedParameter(MaxNumberOfEvaluators.class, maxLocalEvaluators);
    return this;
  }

  /**
   * Builds the configuration.
   * @return The built configuration
   */
  public Configuration build() {
    Validate.notNull(this.submissionRunitme, "Default Runtime was not defined");

    if(!this.defaultRuntime.isPresent() || this.runtimeNames.size() == 1){
      this.defaultRuntime = Optional.of(this.runtimeNames.toArray(new String[0])[0]);
    }

    Validate.isTrue(this.defaultRuntime.isPresent(),
            "Default runtime was not defined, and multiple runtimes were specified");

    if(!this.runtimeNames.contains(this.defaultRuntime.get())){
      this.runtimeNames.add(this.defaultRuntime.get());
    }

    ConfigurationModuleBuilder conf = new MultiRuntimeHelperConfiguration();

    for(Map.Entry<Class, Object> entry: this.namedParameters.entrySet()){
      conf = conf.bindNamedParameter(entry.getKey(), entry.getValue().toString());
    }

    conf = conf.bindNamedParameter(DefaultRuntimeName.class, this.defaultRuntime.get());

    for(final String runtimeName : this.runtimeNames){
      conf = conf.bindSetEntry(RuntimeNames.class, runtimeName);
    }

    conf = conf.bindImplementation(
            MultiRuntimeDefinitionGenerator.class, MultiRuntimeDefinitionGeneratorImpl.class);

    if(!this.submissionRunitme.equalsIgnoreCase(RuntimeIdentifier.RUNTIME_NAME)){
      throw new RuntimeException("Unsupported submission runtime " + this.submissionRunitme);
    }

    // Currently only local runtime is supported as a secondary runtime
    return Configurations.merge(conf.build().build(),
            ExtensibleYarnClientConfiguration.CONF
                    .set(ExtensibleYarnClientConfiguration.DRIVER_CONFIGURATION_PROVIDER,
                            MultiRuntimeDriverConfigurationProvider.class).build());
  }
}
