/*
 * Copyright 2013 Microsoft.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.tests.yarn.failure;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.microsoft.reef.driver.context.ContextConfiguration;
import com.microsoft.reef.driver.evaluator.AllocatedEvaluator;
import com.microsoft.reef.driver.evaluator.EvaluatorRequest;
import com.microsoft.reef.driver.evaluator.EvaluatorRequestor;
import com.microsoft.reef.driver.evaluator.FailedEvaluator;
import com.microsoft.reef.poison.context.PoisonedContextConfiguration;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.time.event.StartTime;

/**
 *
 */
@Unit
public class FailureDriver {

  private static final Logger LOG = Logger.getLogger(FailureDriver.class
    .getName());

  private final EvaluatorRequestor requestor;

  private final AtomicInteger submitted = new AtomicInteger(10);



  @Inject
  public FailureDriver(final EvaluatorRequestor requestor) {
    this.requestor = requestor;
    LOG.info("Driver instantiated");
  }

  /**
   * Handles the StartTime event: Request as single Evaluator.
   */
  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      FailureDriver.this.requestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(40)
          .setMemory(64)
          .build());
      LOG.log(Level.INFO, "Requested Evaluator.");
    }
  }

  /**
   * Handles AllocatedEvaluator: Submit a poisoned context
   */
  final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      if(submitted.getAndDecrement()>0) {
        allocatedEvaluator.submitContext(
          Tang.Factory.getTang()
            .newConfigurationBuilder(
                ContextConfiguration.CONF
                  .set(ContextConfiguration.IDENTIFIER, "Poisoned Context")
                  .build(),
                PoisonedContextConfiguration.CONF
                  .set(PoisonedContextConfiguration.CRASH_PROBABILITY, "1")
                  .set(PoisonedContextConfiguration.CRASH_TIMEOUT, "1")
                  .build()
             ).build());
      }
      else {
        allocatedEvaluator.close();
      }
    }
  }

  /**
   * Handles FailedEvaluator: Resubmits the single Evaluator
   * resource request
   */
  final class FailedEvaluatorHandler implements EventHandler<FailedEvaluator>{

    @Override
    public void onNext(final FailedEvaluator failedEvaluator) {
      LOG.info("Got a failed evaluator " + failedEvaluator.getId());
      FailureDriver.this.requestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(1)
          .setMemory(64)
          .build());
      LOG.log(Level.INFO, "Re-Requested Evaluator.");
    }

  }

}
