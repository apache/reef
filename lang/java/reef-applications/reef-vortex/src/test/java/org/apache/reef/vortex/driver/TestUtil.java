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
package org.apache.reef.vortex.driver;

import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.vortex.api.VortexFunction;
import org.apache.reef.vortex.api.VortexFuture;

import java.io.Serializable;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Utility methods for tests.
 */
public class TestUtil {
  private final AtomicInteger taskletId = new AtomicInteger(0);
  private final AtomicInteger workerId = new AtomicInteger(0);

  /**
   * @return a new mocked worker.
   */
  public VortexWorkerManager newWorker() {
    final RunningTask reefTask = mock(RunningTask.class);
    when(reefTask.getId()).thenReturn("worker" + String.valueOf(workerId.getAndIncrement()));
    final VortexRequestor vortexRequestor = mock(VortexRequestor.class);
    return new VortexWorkerManager(vortexRequestor, reefTask);
  }

  /**
   * @return a new dummy tasklet.
   */
  public Tasklet newTasklet() {
    return new Tasklet(taskletId.getAndIncrement(), null, null, new VortexFuture(Collections.EMPTY_LIST));
  }

  /**
   * @return a new dummy function.
   */
  public VortexFunction newFunction() {
    return new VortexFunction() {
      @Override
      public Serializable call(final Serializable serializable) throws Exception {
        return null;
      }
    };
  }

  /**
   * @return a dummy integer-integer function.
   */
  public VortexFunction<Integer, Integer> newIntegerFunction() {
    return new VortexFunction<Integer, Integer>() {
      @Override
      public Integer call(final Integer integer) throws Exception {
        return 1;
      }
    };
  }
}
