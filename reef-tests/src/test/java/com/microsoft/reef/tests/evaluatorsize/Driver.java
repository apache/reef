package com.microsoft.reef.tests.evaluatorsize;

import com.microsoft.reef.driver.context.ContextConfiguration;
import com.microsoft.reef.driver.evaluator.AllocatedEvaluator;
import com.microsoft.reef.driver.evaluator.EvaluatorRequest;
import com.microsoft.reef.driver.evaluator.EvaluatorRequestor;
import com.microsoft.reef.driver.task.TaskConfiguration;
import com.microsoft.reef.tests.exceptions.DriverSideFailure;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.logging.Logger;

@Unit
final class Driver {
  private static final Logger LOG = Logger.getLogger(Driver.class.getName());

  private final EvaluatorRequestor evaluatorRequestor;

  private final int memorySize;

  @Inject
  public Driver(final EvaluatorRequestor evaluatorRequestor,
                final @Parameter(EvaluatorSizeTestConfiguration.MemorySize.class) int memorySize) {
    this.evaluatorRequestor = evaluatorRequestor;
    this.memorySize = memorySize;
  }

  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      Driver.this.evaluatorRequestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(1)
          .setMemory(Driver.this.memorySize)
          .build());
    }
  }

  final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {

    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {

      final int evaluatorMemory = allocatedEvaluator.getEvaluatorDescriptor().getMemory();

      if (evaluatorMemory < Driver.this.memorySize) {
        throw new DriverSideFailure(
            "Got an Evaluator with too little RAM. Asked for " + Driver.this.memorySize
            + "MB, but got " + evaluatorMemory + "MB.");
      }

      // ALL good on the Driver side. Let's move on to the Task
      try {
        final Configuration contextConfiguration = ContextConfiguration.CONF
            .set(ContextConfiguration.IDENTIFIER, "EvaluatorSizeTest")
            .build();

        final Configuration taskConfiguration = TaskConfiguration.CONF
            .set(TaskConfiguration.TASK, MemorySizeTask.class)
            .set(TaskConfiguration.IDENTIFIER, "EvaluatorSizeTestTask")
            .build();

        final Configuration testConfiguration = EvaluatorSizeTestConfiguration.CONF
            .set(EvaluatorSizeTestConfiguration.MEMORY_SIZE, Driver.this.memorySize)
            .build();

        final Configuration mergedTaskConfiguration = Tang.Factory.getTang()
            .newConfigurationBuilder(taskConfiguration, testConfiguration).build();

        allocatedEvaluator.submitContextAndTask(contextConfiguration, mergedTaskConfiguration);

      } catch (final BindException e) {
        throw new DriverSideFailure("Unable to launch Task", e);
      }
    }
  }
}
