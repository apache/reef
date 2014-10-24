package org.apache.reef.examples.scheduler;

import org.apache.reef.task.Task;
import org.apache.reef.util.CommandUtils;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * <pre>
 * Borrowed code from {@link org.apache.reef.examples.hellohttp.ShellTask}
 * and {@link org.apache.reef.examples.retained_eval.ShellTask}
 * Execute command, capture its stdout, and return that string to the job driver.
 * </pre>
 */
public class ShellTask implements Task {

  /**
   * Standard java logger.
   */
  private static final Logger LOG = Logger.getLogger(ShellTask.class.getName());

  /**
   * A command to execute.
   */
  private final String command;

  /**
   * Task constructor. Parameters are injected automatically by TANG.
   *
   * @param command a command to execute.
   */
  @Inject
  private ShellTask(@Parameter(SchedulerREEF.Command.class) final String command) {
    this.command = command;
  }

  /**
   * Execute the shell command and return the result, which is sent back to
   * the JobDriver and surfaced in the CompletedTask object.
   *
   * @param memento ignored.
   * @return byte string containing the stdout from executing the shell command.
   */
  @Override
  public byte[] call(final byte[] memento) {
    final String result = CommandUtils.runCommand(this.command);
    final String message = "Command : " + command + "\n" + result;

    LOG.log(Level.INFO, message);
    return SchedulerDriver.CODEC.encode(message);
  }
}
