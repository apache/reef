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

import org.apache.reef.runtime.azbatch.client.AzureBatchRuntimeConfiguration;
import org.apache.reef.runtime.azbatch.client.AzureBatchRuntimeConfigurationProvider;
import org.apache.reef.runtime.azbatch.driver.RuntimeIdentifier;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;

import java.io.IOException;

/**
 * A TestEnvironment for the Azure Batch resourcemanager.
 */
public final class AzureBatchTestEnvironment extends TestEnvironmentBase implements TestEnvironment {

  // Used to make sure the tests call the methods in the right order.
  private boolean ready = false;

  @Override
  public synchronized void setUp() {
    this.ready = true;
  }

  @Override
  public synchronized Configuration getRuntimeConfiguration() {
    assert this.ready;
    try {
      Configuration userConfiguration = AzureBatchRuntimeConfiguration.fromEnvironment();
      final Injector injector = Tang.Factory.getTang().newInjector(userConfiguration);
      final AzureBatchRuntimeConfigurationProvider runtimeConfigurationProvider =
          injector.getInstance(AzureBatchRuntimeConfigurationProvider.class);
      return runtimeConfigurationProvider.getAzureBatchRuntimeConfiguration();
    } catch (IOException | InjectionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public synchronized void tearDown() {
    assert this.ready;
    this.ready = false;
  }

  @Override
  public int getTestTimeout() {
    return 5 * 60000; // 5 min.
  }

  @Override
  public String getRuntimeName() {
    return RuntimeIdentifier.RUNTIME_NAME;
  }
}
