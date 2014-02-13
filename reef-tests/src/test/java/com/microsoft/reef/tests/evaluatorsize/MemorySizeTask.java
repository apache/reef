package com.microsoft.reef.tests.evaluatorsize;

import com.microsoft.reef.task.Task;
import com.microsoft.reef.tests.exceptions.TaskSideFailure;
import com.microsoft.tang.annotations.Parameter;

import javax.inject.Inject;

final class MemorySizeTask implements Task {

  private final int memorySize;
  private static final int MEGA = 1048576;
  private static final int allowedDelta = 128; // TODO: This should'nt be necessary. Could be the perm size we set.

  @Inject
  public MemorySizeTask(final @Parameter(EvaluatorSizeTestConfiguration.MemorySize.class) int memorySize) {
    this.memorySize = memorySize;
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {
    final long maxHeapSizeInBytes = Runtime.getRuntime().maxMemory();

    final long maxHeapSizeMB = maxHeapSizeInBytes / MEGA;
    if (maxHeapSizeMB < (this.memorySize - allowedDelta)) {
      throw new TaskSideFailure("Got an Evaluator with too little RAM. Asked for " + this.memorySize
          + "MB, but got " + maxHeapSizeMB + "MB.");
    }
    return new byte[0];
  }
}
