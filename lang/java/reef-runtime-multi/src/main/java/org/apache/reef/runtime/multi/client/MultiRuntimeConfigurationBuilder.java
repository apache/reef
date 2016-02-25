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

import org.apache.reef.runtime.local.client.parameters.MaxNumberOfEvaluators;
import org.apache.reef.runtime.yarn.client.ExtensibleYarnClientConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.formats.ConfigurationModule;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;
import java.util.TreeSet;

/**
 * A builder for Multi Runtime Configuration.
 */
public final class MultiRuntimeConfigurationBuilder {
  private static final Set<String> SUPPORTED_RUNTIMES = new TreeSet<>(Arrays.asList(
          org.apache.reef.runtime.yarn.driver.RuntimeIdentifier.RUNTIME_NAME,
          org.apache.reef.runtime.local.driver.RuntimeIdentifier.RUNTIME_NAME));
  private static final Set<String> SUPPORTED_DEFAULT_RUNTIMES = new TreeSet<>(Arrays.asList(
          org.apache.reef.runtime.yarn.driver.RuntimeIdentifier.RUNTIME_NAME));

  private Set<String> runtimeNames = new TreeSet<>();
  private String defaultRuntime = null;
  private final HashMap<Class, Object> namedParameters = new HashMap<>();

  private void addNamedParameter(final Class namedParameter,
                                           final Object namedParameterValue) {
    if (namedParameterValue == null) {
      throw new IllegalArgumentException("Named parameter value cannot be null");
    }

    this.namedParameters.put(namedParameter, namedParameterValue);
  }

  public MultiRuntimeConfigurationBuilder addRuntime(final String runtimeName) {
    if (!SUPPORTED_RUNTIMES.contains(runtimeName)) {
      throw new IllegalArgumentException("Unsupported Runtime " + runtimeName);
    }

    this.runtimeNames.add(runtimeName);
    return this;
  }

  public MultiRuntimeConfigurationBuilder addDefaultRuntime(final String runtimeName) {
    if (!SUPPORTED_DEFAULT_RUNTIMES.contains(runtimeName)) {
      throw new IllegalArgumentException("Unsupported Default Runtime " + runtimeName);
    }

    if (this.defaultRuntime != null) {
      throw new IllegalArgumentException("Default runtime was already added");
    }

    this.defaultRuntime = runtimeName;
    return this;
  }

  public MultiRuntimeConfigurationBuilder setMaxEvaluatorsNumberForLocalRuntime(final int maxLocalEvaluators) {
    if (maxLocalEvaluators < 1) {
      throw new IllegalArgumentException("Max evaluators number shoudl be greater then 1");
    }

    addNamedParameter(MaxNumberOfEvaluators.class, maxLocalEvaluators);
    return this;
  }

  public Configuration build() {
    if (defaultRuntime == null) {
      throw new IllegalStateException("Default Runtime was not defined");
    }

    // Currently only Yarn default runtime is supported
    // Remove default runtime from the list
    this.runtimeNames.remove(this.defaultRuntime);

    // Currently only local runtime is supported as a secondary runtime
    ConfigurationModule cm = ExtensibleYarnClientConfiguration.getConfigurationModule(this.namedParameters)
            .set(ExtensibleYarnClientConfiguration.DRIVER_CONFIGURATION_PROVIDER,
                    MultiRuntimeYarnLocalDriverConfigurationProviderImpl.class);

    return cm.build();
  }
}
