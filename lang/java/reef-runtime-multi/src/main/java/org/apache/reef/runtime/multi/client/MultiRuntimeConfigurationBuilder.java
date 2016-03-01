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
import org.apache.reef.runtime.yarn.client.ExtensibleYarnClientConfiguration;
import org.apache.reef.tang.Configuration;

import java.util.*;

/**
 * A builder for Multi Runtime Configuration.
 */
public final class MultiRuntimeConfigurationBuilder {
  private static final Set<String> SUPPORTED_RUNTIMES = new HashSet<>(Arrays.asList(
          org.apache.reef.runtime.yarn.driver.RuntimeIdentifier.RUNTIME_NAME,
          org.apache.reef.runtime.local.driver.RuntimeIdentifier.RUNTIME_NAME));
  private static final Set<String> SUPPORTED_DEFAULT_RUNTIMES = new HashSet<>(Arrays.asList(
          org.apache.reef.runtime.yarn.driver.RuntimeIdentifier.RUNTIME_NAME));

  private Set<String> runtimeNames = new HashSet<>();
  private String defaultRuntime = null;
  private final HashMap<Class, Object> namedParameters = new HashMap<>();

  private void addNamedParameter(final Class namedParameter,
                                 final Object namedParameterValue) {
    Validate.notNull(namedParameterValue);

    this.namedParameters.put(namedParameter, namedParameterValue);
  }

  public MultiRuntimeConfigurationBuilder addRuntime(final String runtimeName) {
    Validate.isTrue(SUPPORTED_RUNTIMES.contains(runtimeName), "unsupported runtime " + runtimeName);

    this.runtimeNames.add(runtimeName);
    return this;
  }

  public MultiRuntimeConfigurationBuilder setDefaultRuntime(final String runtimeName) {
    Validate.isTrue(SUPPORTED_DEFAULT_RUNTIMES.contains(runtimeName), "Unsupported primary runtime " + runtimeName);
    Validate.isTrue(this.defaultRuntime == null, "Default runtime was already added");

    this.defaultRuntime = runtimeName;
    return this;
  }

  public MultiRuntimeConfigurationBuilder setMaxEvaluatorsNumberForLocalRuntime(final int maxLocalEvaluators) {
    Validate.isTrue(maxLocalEvaluators > 0, "Max evaluators number shoudl be greater then 0");

    addNamedParameter(MaxNumberOfEvaluators.class, maxLocalEvaluators);
    return this;
  }

  public Configuration build() {
    Validate.notNull(this.defaultRuntime, "Default Runtime was not defined");

    // Currently only Yarn default runtime is supported
    // Remove default runtime from the list
    this.runtimeNames.remove(this.defaultRuntime);

    // Currently only local runtime is supported as a secondary runtime
    return ExtensibleYarnClientConfiguration.getConfigurationModule(this.namedParameters)
            .set(ExtensibleYarnClientConfiguration.DRIVER_CONFIGURATION_PROVIDER,
                    MultiRuntimeYarnLocalDriverConfigurationProviderImpl.class).build();
  }
}
