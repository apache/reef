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
package org.apache.reef.tests;

import org.apache.reef.io.ConfigurableDirectoryTempFileCreator;
import org.apache.reef.io.TempFileCreator;
import org.apache.reef.io.parameters.TempFileRootFolder;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.runtime.local.driver.RuntimeIdentifier;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;

/**
 * A TestEnvironment for the local resourcemanager.
 */
public final class LocalTestEnvironment extends TestEnvironmentBase implements TestEnvironment {

  /**
   * The upper limit on the number of Evaluators that the local resourcemanager will hand out concurrently.
   */
  public static final int MAX_NUMBER_OF_EVALUATORS = 4;
  // Used to make sure the tests call the methods in the right order.
  private boolean ready = false;

  @Override
  public synchronized void setUp() {
    this.ready = true;
  }

  @Override
  public synchronized Configuration getRuntimeConfiguration() {
    assert this.ready;
    final String rootFolder = System.getProperty("org.apache.reef.runtime.local.folder");
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(TempFileRootFolder.class, "./target/reef/temp");
    jcb.bindImplementation(TempFileCreator.class, ConfigurableDirectoryTempFileCreator.class);
    if (null == rootFolder) {
      return Configurations.merge(jcb.build(), LocalRuntimeConfiguration.CONF
          .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, MAX_NUMBER_OF_EVALUATORS)
          .set(LocalRuntimeConfiguration.RUNTIME_ROOT_FOLDER, "target/REEF_LOCAL_RUNTIME")
          .build());
    } else {
      return Configurations.merge(jcb.build(), LocalRuntimeConfiguration.CONF
          .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, MAX_NUMBER_OF_EVALUATORS)
          .set(LocalRuntimeConfiguration.RUNTIME_ROOT_FOLDER, rootFolder)
          .build());
    }
  }

  @Override
  public synchronized void tearDown() {
    assert this.ready;
    this.ready = false;
  }

  @Override
  public int getTestTimeout() {
    return 120000; // 2 min.
  }

  @Override
  public String getRuntimeName() {
    return RuntimeIdentifier.RUNTIME_NAME;
  }
}
