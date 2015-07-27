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

import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tests.configurationproviders.parameters.DriverAndEvaluatorParameter;
import org.apache.reef.tests.configurationproviders.parameters.DriverParameter;
import org.apache.reef.tests.library.exceptions.DriverSideFailure;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;

@Unit
final class ConfigurationProviderTestDriver {
  private final EvaluatorRequestor evaluatorRequestor;
  private final Injector injector;

  @Inject
  private ConfigurationProviderTestDriver(final EvaluatorRequestor evaluatorRequestor, final Injector injector) {
    this.evaluatorRequestor = evaluatorRequestor;
    this.injector = injector;
  }


  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {

      // Check that the parameters were set
      checkParameter(DriverParameter.class, DriverParameter.TEST_VALUE);
      checkParameter(DriverAndEvaluatorParameter.class, DriverAndEvaluatorParameter.TEST_VALUE);

      // If we got here, the parameters were set correctly and we can proceed.
      evaluatorRequestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(1)
          .setMemory(32)
          .build());
    }


  }

  final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {

    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      allocatedEvaluator.submitTask(TaskConfiguration.CONF
          .set(TaskConfiguration.TASK, ConfigurationProviderTestTask.class)
          .set(TaskConfiguration.IDENTIFIER, "ConfigurationProviderTestTask")
          .build());
    }
  }

  private void checkParameter(final Class<? extends Name<String>> name, final String expectedValue) {
    try {
      final String value = injector.getNamedInstance(name);
      if (!expectedValue.equals(value)) {
        throw new DriverSideFailure(name.toString() + " not set correctly");
      }
    } catch (final InjectionException e) {
      throw new DriverSideFailure(name.toString() + " not set.", e);
    }
  }
}
