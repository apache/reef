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
package org.apache.reef.tests.rack.awareness;

import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.CompletedEvaluator;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tests.library.exceptions.DriverSideFailure;
import org.apache.reef.tests.rack.awareness.RackAwareTask;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StopTime;
import org.junit.Assert;

import javax.inject.Inject;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

@Unit
final class RackAwareEvaluatorTestDriver {

  private static final Logger LOG = Logger.getLogger(RackAwareEvaluatorTestDriver.class.getName());
  private final AtomicBoolean completedEvaluatorReceived = new AtomicBoolean(false);
  // hardcoded not to use the constant in ResourceCatalogImpl, which is local runtime dependent
  // TODO future iterations so change this
  private final String DEFAULT_RACK = "/default-rack";

  @Inject
  RackAwareEvaluatorTestDriver() {
  }

  /**
   * Verifies whether the rack received is the default rack
   */
  final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {

    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {

      final String actual = allocatedEvaluator.getEvaluatorDescriptor().getNodeDescriptor().getRackDescriptor().getName();
      Assert.assertEquals(DEFAULT_RACK, actual);

      LOG.info("Received rack name " + actual);

      final Configuration configuration =
          Tang.Factory.getTang().newConfigurationBuilder(
              TaskConfiguration.CONF
                .set(TaskConfiguration.IDENTIFIER, "RackAwareEvaluatorTask")
                .set(TaskConfiguration.TASK, RackAwareTask.class)
                .build())
              .bindNamedParameter(RackNameParameter.class, actual)
              .build();
      allocatedEvaluator.submitTask(configuration);
    }
  }

  final class EvaluatorCompleteHandler implements EventHandler<CompletedEvaluator> {

    @Override
    public void onNext(final CompletedEvaluator completedEvaluator) {
      LOG.log(Level.INFO, "Received a CompletedEvaluator for Evaluator {0}", completedEvaluator.getId());
      completedEvaluatorReceived.set(true);
    }
  }

  final class StopHandler implements EventHandler<StopTime> {

    @Override
    public void onNext(final StopTime stopTime) {
      synchronized (completedEvaluatorReceived) {
        if (completedEvaluatorReceived.get()) {
          LOG.info("Received an expected CompletedEvaluator. All good.");
        } else {
          LOG.info("Did not receive an expected CompletedEvaluator.");
          throw new DriverSideFailure("Did not receive an expected CompletedEvaluator.");
        }
      }
    }
  }

}
