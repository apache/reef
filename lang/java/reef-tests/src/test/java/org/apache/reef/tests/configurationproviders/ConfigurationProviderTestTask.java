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
package org.apache.reef.tests.configurationproviders;

import org.apache.reef.tang.Injector;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.task.Task;
import org.apache.reef.tests.configurationproviders.parameters.DriverAndEvaluatorParameter;
import org.apache.reef.tests.configurationproviders.parameters.EvaluatorParameter;
import org.apache.reef.tests.library.exceptions.TaskSideFailure;

import javax.inject.Inject;

final class ConfigurationProviderTestTask implements Task {

  private final Injector injector;

  @Inject
  private ConfigurationProviderTestTask(final Injector injector) {
    this.injector = injector;
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {

    // Check the parameters
    checkParameter(EvaluatorParameter.class, EvaluatorParameter.TEST_VALUE);
    checkParameter(DriverAndEvaluatorParameter.class, DriverAndEvaluatorParameter.TEST_VALUE);
    return new byte[0];
  }

  private void checkParameter(final Class<? extends Name<String>> name, final String expectedValue) {
    try {
      final String value = injector.getNamedInstance(name);
      if (!expectedValue.equals(value)) {
        throw new TaskSideFailure(name.toString() + " not set correctly");
      }
    } catch (final InjectionException e) {
      throw new TaskSideFailure(name.toString() + " not set.", e);
    }
  }
}
