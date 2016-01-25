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
package org.apache.reef.tests.evaluatorsize;

import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.Task;
import org.apache.reef.tests.library.exceptions.TaskSideFailure;

import javax.inject.Inject;

final class MemorySizeTask implements Task {

  private static final int MEGA = 1048576;
  private static final int ALLOWED_DELTA = 128; // This shouldn't be necessary. Could be the perm size we set.
  private final int memorySize;

  @Inject
  MemorySizeTask(@Parameter(EvaluatorSizeTestConfiguration.MemorySize.class) final int memorySize) {
    this.memorySize = memorySize;
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {
    final long maxHeapSizeInBytes = Runtime.getRuntime().maxMemory();

    final long maxHeapSizeMB = maxHeapSizeInBytes / MEGA;
    if (maxHeapSizeMB < (this.memorySize - ALLOWED_DELTA)) {
      throw new TaskSideFailure("Got an Evaluator with too little RAM. Asked for " + this.memorySize
          + "MB, but got " + maxHeapSizeMB + "MB.");
    }
    return new byte[0];
  }
}
