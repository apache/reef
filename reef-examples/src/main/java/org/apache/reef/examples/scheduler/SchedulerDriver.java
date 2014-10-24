package org.apache.reef.examples.scheduler;

import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.task.CompletedTask;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.impl.ObjectSerializableCodec;
import org.apache.reef.wake.time.event.StartTime;

import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Driver for TaskScheduler. It receives the commands by HttpRequest and
 * execute them in a FIFO(First In First Out) order.
 */
@Unit
public final class SchedulerDriver {

  public static final ObjectSerializableCodec<String> CODEC = new ObjectSerializableCodec<>();
  private static final Logger LOG = Logger.getLogger(SchedulerDriver.class.getName());

  /**
   * Possible states of the job driver. Can be one of:
   * <dl>
   * <du><code>INIT</code></du><dd>Initial state. Ready to request an evaluator</dd>
   * <du><code>WAIT_EVALUATORS</code></du><dd>Wait for requested evaluators to be ready.</dd>
   * <du><code>READY</code></du><dd>Wait for the commands. When new Tasks arrive, enqueue the tasks and transit to RUNNING status.</dd>
   * <du><code>RUNNING</code></du><dd>Run commands in the queue. Go back to READY state when the queue is empty.</dd>
   * </dl>
   */
  private enum State {
    INIT, WAIT_EVALUATOR, READY, RUNNING
  }

  /**
   * If true, it reuses evaluators when Tasks done.
   */
  private boolean retainable;

  private Object lock = new Object();

  @GuardedBy("lock")
  private State state = State.INIT;

  @GuardedBy("lock")
  private final Queue<String> taskQueue;

  private final EvaluatorRequestor requestor;
  private final HttpServerShellCmdHandler.CallbackHandler callbackHandler;

  /**
   * Counts how many tasks have been scheduled.
   */
  private AtomicInteger taskCount = new AtomicInteger(0);

  @Inject
  public SchedulerDriver(final EvaluatorRequestor requestor,
                         final HttpServerShellCmdHandler.CallbackHandler callbackHandler,
                         @Parameter(SchedulerREEF.Retain.class) boolean retainable) {
    this.requestor = requestor;
    this.callbackHandler = callbackHandler;

    this.taskQueue = new LinkedList<>();
    this.retainable = retainable;
  }

  /**
   * The driver is ready to run.
   */
  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      LOG.log(Level.INFO, "Driver started at {0}", startTime);
      assert (state == State.INIT);

      requestEvaluator();
    }
  }

  /**
   * Evaluator is allocated. This occurs every time to run commands in Non-retainable version,
   * while occurs only once in the Retainable version
   */
  final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator evaluator) {
      synchronized (lock) {
        LOG.log(Level.INFO, "Evaluator is ready");
        assert (state == State.WAIT_EVALUATOR);

        evaluator.submitContext(ContextConfiguration.CONF
          .set(ContextConfiguration.IDENTIFIER, "SchedulerContext")
          .build());
      }
    }
  }

  /**
   * Now it is ready to schedule tasks. But if the queue is empty,
   * wait until commands coming up.
   */
  final class ActiveContextHandler implements EventHandler<ActiveContext> {
    @Override
    public void onNext(ActiveContext context) {
      synchronized (lock) {
        LOG.log(Level.INFO, "Context available : {0}", context.getId());
        assert (state == State.WAIT_EVALUATOR);

        state = State.READY;
        waitForCommands(context);
      }
    }
  }

  /**
   * Parse the message and enqueue the commands when arrives.
   */
  final class CommandRequestHandler implements EventHandler<byte[]> {
    @Override
    public void onNext(byte[] message) {
      synchronized (lock) {
        LOG.log(Level.INFO, "Command request arrives");
        final int count = parseCommands(message);

        // Report back to the Client.
        callbackHandler.onNext(CODEC.encode(count + " commands arrived. The queue size is " + taskQueue.size() + ".\n"));

        if (readyToRun()) {
          state = State.RUNNING;
          lock.notify();
        }
      }
    }
  }

  /**
   * Non-retainable version of CompletedTaskHandler.
   * When Task completes, it closes the active context to deallocate the evaluator
   * and if there is outstanding commands, allocate another evaluator.
   */
  final class CompletedTaskHandler implements EventHandler<CompletedTask> {
    @Override
    public void onNext(final CompletedTask task) {
      synchronized (lock) {
        LOG.log(Level.INFO, "Task completed. Reuse the evaluator :", String.valueOf(retainable));

        if (retainable) {
          if (taskQueue.isEmpty()) {
            state = State.READY;
          }
          waitForCommands(task.getActiveContext());
        } else {
          task.getActiveContext().close();
          state = State.WAIT_EVALUATOR;
          requestEvaluator();
        }
      }
    }
  }

  /**
   * Request an evaluator
   */
  private synchronized void requestEvaluator() {
    requestor.submit(EvaluatorRequest.newBuilder()
      .setMemory(128)
      .setNumber(1)
      .build());
  }

  /**
   * @param command The command to execute
   */
  private void submit(final ActiveContext context, final String command) {
    // Increment the Task count to track how many tasks are generated.
    final int taskId = taskCount.incrementAndGet();

    final Configuration taskConf = TaskConfiguration.CONF
      .set(TaskConfiguration.TASK, ShellTask.class)
      .set(TaskConfiguration.IDENTIFIER, "ShellTask"+taskId)
      .build();
    final Configuration commandConf = Tang.Factory.getTang().newConfigurationBuilder()
      .bindNamedParameter(SchedulerREEF.Command.class, command)
      .build();

    LOG.log(Level.INFO, "Submitting command : {0}", command);
    final Configuration merged = Configurations.merge(taskConf, commandConf);
    context.submitTask(merged);
  }

  /**
   * Parse the message and enqueue into the taskQueue
   * @param message Message received from the HTTP request.
   * @return The number of commands included in the message.
   */
  private int parseCommands(final byte[] message) {
    // Commands are separated by ; character
    final String[] commands = new String(message).split(";");
    int count = 0;
    for (String command : commands) {
      if (command.length() > 0) {
        taskQueue.add(command);
        count++;
      }
    }
    return count;
  }

  /**
   * Pick up a command from the queue and run it. Wait until
   * any command coming up if no command exists.
   * @param context
   */
  private void waitForCommands(final ActiveContext context) {
    while (taskQueue.isEmpty()) {
      // Wait until any commands enter in the queue
      try {
        lock.wait();
      } catch (InterruptedException e) {
        LOG.log(Level.WARNING, "InterruptedException occurred in SchedulerDriver", e);
      }
    }

    // Run the first command from the queue.
    final String command = taskQueue.poll();
    submit(context, command);
  }

  /**
   * @return {@code true} if it is possible to run commands.
   */
  private boolean readyToRun() {
    return state == State.READY && taskQueue.size() > 0;
  }
}
