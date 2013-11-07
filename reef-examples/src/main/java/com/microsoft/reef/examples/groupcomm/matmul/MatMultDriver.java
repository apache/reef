/**
 * Copyright (C) 2013 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.examples.groupcomm.matmul;

import com.microsoft.reef.driver.activity.*;
import com.microsoft.reef.driver.contexts.*;
import com.microsoft.reef.driver.evaluator.*;
import com.microsoft.reef.examples.utils.wake.BlockingEventHandler;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Driver code for the Matrix Multiplication REEF Application
 */
@Unit
public final class MatMultDriver {

  /**
   * Standard Java logger object.
   */
  private final Logger LOG = Logger.getLogger(MatMultDriver.class.getName());

  /**
   * The number of compute activities to be spawned
   */
  private final int computeActivities;

  /**
   * The sole Control Activity
   */
  private static final int controllerActivities = 1;

  /**
   * Track the number of compute activities that are running
   */
  private final AtomicInteger compActRunning = new AtomicInteger(0);

  /**
   * Activity submission is delegated to
   */
  private final ActivitySubmitter activitySubmitter;

  /**
   * Blocks till all evaluators are available and submits activities using
   * activity submitter. First all the compute activities are submitted. The
   * the control activity is submitted
   */
  private final BlockingEventHandler<ActiveContext> contextAccumulator;

  /**
   * Request evaluators using this
   */
  private final EvaluatorRequestor requestor;


  public static class Parameters {
    @NamedParameter(default_value = "5", doc = "The number of compute activities to spawn")
    public static class ComputeActivities implements Name<Integer> {
    }

    @NamedParameter(default_value = "5678", doc = "Port on which Name Service should listen")
    public static class NameServicePort implements Name<Integer> {
    }
  }

  /**
   * This class is instantiated by TANG
   *
   * @param requestor          evaluator requestor object used to create new evaluator
   *                           containers.
   * @param _computeActivities - named parameter
   * @param nameServicePort    - named parameter
   */
  @Inject
  public MatMultDriver(
      final EvaluatorRequestor requestor,
      @Parameter(Parameters.ComputeActivities.class) int _computeActivities,
      @Parameter(Parameters.NameServicePort.class) int nameServicePort) {
    this.requestor = requestor;
    this.computeActivities = _computeActivities;
    activitySubmitter = new ActivitySubmitter(this.computeActivities,
        nameServicePort);
    contextAccumulator = new BlockingEventHandler<>(computeActivities
        + controllerActivities, activitySubmitter);
  }

  /**
   * Evaluator allocated.
   */
  final class AllocatedEvaluatorHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public final void onNext(final AllocatedEvaluator eval) {
      LOG.log(Level.INFO, "Received an AllocatedEvaluator. Submitting it.");
      try {
        eval.submitContext(ContextConfiguration.CONF.set(ContextConfiguration.IDENTIFIER, "MatMult").build());
      } catch (final BindException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Activity is running. Track the compute activities that are running.
   * Once all compute activities are running submitActivity the ControllerActivity.
   */
  final class RunningActivityHandler implements EventHandler<RunningActivity> {
    @Override
    public final void onNext(final RunningActivity act) {
      LOG.log(Level.INFO, "Activity \"{0}\" is running!", act.getId());
      if (compActRunning.incrementAndGet() == computeActivities) {
        // All compute activities are running
        // Launch controller activity
        activitySubmitter.submitControlActivity();
      }
    }
  }

  /**
   * Activity has completed successfully.
   */
  final class CompletedActivityHandler implements EventHandler<CompletedActivity> {
    @Override
    @SuppressWarnings("ConvertToTryWithResources")
    public final void onNext(final CompletedActivity completed) {
      LOG.log(Level.INFO, "Activity {0} is done.", completed.getId()
          .toString());
      if (activitySubmitter.controllerCompleted(completed.getId())) {
        // Get results from controller
        System.out.println("****************** RESULT ******************");
        System.out.println(new String(completed.get()));
        System.out.println("********************************************");
      }
      final ActiveContext context = completed.getActiveContext();
      LOG.log(Level.INFO, "Releasing Context {0}.", context.getId()
          .toString());
      context.close();
    }
  }

  final class ActiveContextHandler implements EventHandler<ActiveContext> {
    @Override
    public void onNext(ActiveContext activeContext) {
      LOG.log(Level.INFO, "Received a RunningEvaluator with ID: {0}", activeContext.getId());
      contextAccumulator.onNext(activeContext);
    }
  }

  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      LOG.log(Level.INFO, "StartTime: {0}", startTime);
      MatMultDriver.this.requestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(computeActivities + controllerActivities)
          .setSize(EvaluatorRequest.Size.SMALL).build());
    }

    @Override
    public String toString() {
      return "HelloDriver.StartHandler";
    }
  }
}
