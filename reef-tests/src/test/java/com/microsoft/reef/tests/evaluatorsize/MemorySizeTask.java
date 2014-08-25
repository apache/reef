/**
 * Copyright (C) 2014 Microsoft Corporation
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
package com.microsoft.reef.tests.evaluatorsize;

import com.microsoft.reef.task.Task;
import com.microsoft.reef.tests.library.exceptions.TaskSideFailure;
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
