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

import org.apache.reef.runtime.common.files.RuntimeClasspathProvider;
import org.apache.reef.runtime.yarn.YarnClasspathProvider;
import org.apache.reef.runtime.yarn.util.YarnConfigurationConstructor;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;

import javax.inject.Inject;

/**
 * Yarn implementation for MultiRuntimeMainConfigurationGenerator.
 */
final class YarnMultiRuntimeMainConfigurationGeneratorImpl
        implements MultiRuntimeMainConfigurationGenerator {

  @Inject
  private YarnMultiRuntimeMainConfigurationGeneratorImpl() {
  }

  /**
   * Generates configuration that allows multi runtime to run on Yarn.
   * MultiRuntimeMainConfigurationGenerator.
   * @return Instance of <code>Configuration</code>
   */
  @Override
  public Configuration getMainConfiguration() {

    return Tang.Factory.getTang().newConfigurationBuilder()
            .bindImplementation(RuntimeClasspathProvider.class, YarnClasspathProvider.class)
            .bindConstructor(org.apache.hadoop.yarn.conf.YarnConfiguration.class, YarnConfigurationConstructor.class)
            .build();
  }
}
