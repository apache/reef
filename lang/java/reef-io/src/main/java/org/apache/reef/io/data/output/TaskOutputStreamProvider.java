package org.apache.reef.io.data.output;

/**
 * A provider through which users create task output streams.
 */
public abstract class TaskOutputStreamProvider implements OutputStreamProvider {

  /**
   * id of the current task.
   */
  private String taskId;

  /**
   * set the id of the current task.
   *
   * @param taskId id of the current task
   */
  protected final void setTaskId(final String taskId) {
    this.taskId = taskId;
  }

  /**
   * get the id of the current task.
   *
   * @return id of the current task
   */
  protected final String getTaskId() {
    return this.taskId;
  }
}
