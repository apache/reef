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

import org.apache.reef.annotations.Unstable;
import org.apache.reef.annotations.audience.RuntimeAuthor;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * Defines a contract for a multi runtime main configuration.
 * Main configuration is the configuration  that allows multi runtime to be initialized in the certain concrete
 * environment. For example, when operating in  the yarn runtime, multi runtime needs to receive the proper
 * classpath provider as well as the constructor for the YarnConfiguration.
 * classpath provider as well as the constructor for the YarnConfiguration.
 * Main configuration is provided in the context of the main runtime. Main runtime is the runtime that actually runs
 * multi runtime host that hosts the actual runtimes.
 *The main configuration is generated on the client and is merged into the driver configuration.
 */
@DefaultImplementation(EmptyMultiRuntimeMainConfigurationGeneratorImpl.class)
@Unstable
@RuntimeAuthor
interface MultiRuntimeMainConfigurationGenerator {
  /**
   * Generates needed driver configuration such as class path provider.
   * @return Instance of <code>Configuration</code>
   */
  Configuration getMainConfiguration();
}
