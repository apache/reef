package com.microsoft.reef.runtime.common.driver;

import com.microsoft.reef.client.DriverConfigurationOptions;
import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.context.ClosedContext;
import com.microsoft.reef.driver.context.ContextMessage;
import com.microsoft.reef.driver.context.FailedContext;
import com.microsoft.reef.driver.evaluator.AllocatedEvaluator;
import com.microsoft.reef.driver.evaluator.CompletedEvaluator;
import com.microsoft.reef.driver.evaluator.FailedEvaluator;
import com.microsoft.reef.driver.task.*;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.time.runtime.event.RuntimeStart;

import javax.inject.Inject;
import java.util.Set;

/**
 * A class that does nothing but depend on all the Evaluator and Task handlers given by the user.
 * Reasoning: This way we ensure that they are instantiated once, before we fork the injectors.
 */
final class DriverRuntimeStartHandler implements EventHandler<RuntimeStart> {
  @Inject
  private DriverRuntimeStartHandler(final @Parameter(DriverConfigurationOptions.ActiveContextHandlers.class) Set<EventHandler<ActiveContext>> activeContextEventHandlers,
                                    final @Parameter(DriverConfigurationOptions.ClosedContextHandlers.class) Set<EventHandler<ClosedContext>> closedContextEventHandlers,
                                    final @Parameter(DriverConfigurationOptions.FailedContextHandlers.class) Set<EventHandler<FailedContext>> failedContextEventHandlers,
                                    final @Parameter(DriverConfigurationOptions.ContextMessageHandlers.class) Set<EventHandler<ContextMessage>> contextMessageHandlers,
                                    final @Parameter(DriverConfigurationOptions.RunningTaskHandlers.class) Set<EventHandler<RunningTask>> runningTaskEventHandlers,
                                    final @Parameter(DriverConfigurationOptions.CompletedTaskHandlers.class) Set<EventHandler<CompletedTask>> completedTaskEventHandlers,
                                    final @Parameter(DriverConfigurationOptions.SuspendedTaskHandlers.class) Set<EventHandler<SuspendedTask>> suspendedTaskEventHandlers,
                                    final @Parameter(DriverConfigurationOptions.TaskMessageHandlers.class) Set<EventHandler<TaskMessage>> taskMessageEventHandlers,
                                    final @Parameter(DriverConfigurationOptions.FailedTaskHandlers.class) Set<EventHandler<FailedTask>> taskExceptionEventHandlers,
                                    final @Parameter(DriverConfigurationOptions.AllocatedEvaluatorHandlers.class) Set<EventHandler<AllocatedEvaluator>> allocatedEvaluatorEventHandlers,
                                    final @Parameter(DriverConfigurationOptions.FailedEvaluatorHandlers.class) Set<EventHandler<FailedEvaluator>> failedEvaluatorHandlers,
                                    final @Parameter(DriverConfigurationOptions.CompletedEvaluatorHandlers.class) Set<EventHandler<CompletedEvaluator>> completedEvaluatorHandlers) {
  }

  @Override
  public void onNext(RuntimeStart runtimeStart) {

  }
}
