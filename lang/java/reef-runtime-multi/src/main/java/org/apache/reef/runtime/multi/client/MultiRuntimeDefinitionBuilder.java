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
import org.apache.commons.lang.StringUtils;
import org.apache.reef.runtime.multi.utils.avro.MultiRuntimeDefinition;
import org.apache.reef.runtime.multi.utils.avro.RuntimeDefinition;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.apache.reef.tang.formats.ConfigurationModule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Builder for multi runtime definition.
 */
public final class MultiRuntimeDefinitionBuilder {
  private Map<String, RuntimeDefinition> runtimes = new HashMap<>();
  private String defaultRuntime;

  private static RuntimeDefinition createRuntimeDefinition(final ConfigurationModule configModule,
                                                             final String runtimeName) {
    final Configuration localDriverConfiguration = configModule.build();
    final AvroConfigurationSerializer serializer = new AvroConfigurationSerializer();
    final String serializedConfig = serializer.toString(localDriverConfiguration);
    return new RuntimeDefinition(runtimeName, serializedConfig);
  }

  /**
   * Adds runtime configuration module to the builder.
   * @param config The configuration module
   * @param runtimeName The name of teh runtime
   * @return The builder instance
   */
  public MultiRuntimeDefinitionBuilder addRuntime(final ConfigurationModule config, final String runtimeName){
    Validate.notNull(config, "runtime configuration module should not be null");
    Validate.isTrue(!StringUtils.isEmpty(runtimeName) && !StringUtils.isBlank(runtimeName));
    RuntimeDefinition rd = createRuntimeDefinition(config, runtimeName);
    this.runtimes.put(runtimeName, rd);
    return this;
  }

  /**
   * Sets default runtime name.
   * @param runtimeName The name of teh default runtime
   * @return The builder instance
   */
  public MultiRuntimeDefinitionBuilder setDefaultRuntimeName(final String runtimeName){
    Validate.isTrue(!StringUtils.isEmpty(runtimeName) && !StringUtils.isBlank(runtimeName), "runtimeName " +
            "should" +
            " be non empty and non blank string");
    this.defaultRuntime = runtimeName;
    return this;
  }

  /**
   * Builds multi runtime definition.
   * @return The populated definition object
   */
  public MultiRuntimeDefinition build(){
    Validate.isTrue(this.runtimes.size() == 1 || !StringUtils.isEmpty(this.defaultRuntime), "Default runtime " +
            "should be set if more then single runtime provided");

    if(StringUtils.isEmpty(this.defaultRuntime)){
      // we have single runtime configured, take its name as a default
      this.defaultRuntime = this.runtimes.keySet().iterator().next();
    }

    Validate.isTrue(this.runtimes.containsKey(this.defaultRuntime), "Default runtime shoudl be configured");
    return new MultiRuntimeDefinition(defaultRuntime, new ArrayList<>(this.runtimes.values()));
  }
}
