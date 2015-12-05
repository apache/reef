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
import org.apache.reef.vortex.common.TaskletCancellationRequest;
import org.apache.reef.vortex.common.VortexRequest;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.Serializable;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Utility methods for tests.
 */
public final class TestUtil {
  private final AtomicInteger taskletId = new AtomicInteger(0);
  private final AtomicInteger workerId = new AtomicInteger(0);
  private final Executor executor = Executors.newFixedThreadPool(5);
  private final VortexMaster vortexMaster = mock(VortexMaster.class);

  /**
   * @return a new mocked worker.
   */
  public VortexWorkerManager newWorker() {
    final RunningTask reefTask = mock(RunningTask.class);
    when(reefTask.getId()).thenReturn("worker" + String.valueOf(workerId.getAndIncrement()));
    final VortexRequestor vortexRequestor = mock(VortexRequestor.class);
    final VortexWorkerManager workerManager = new VortexWorkerManager(vortexRequestor, reefTask);
    doAnswer(new Answer() {
      @Override
      public Object answer(final InvocationOnMock invocation) throws Throwable {
        final VortexRequest request = (VortexRequest)invocation.getArguments()[1];
        if (request instanceof TaskletCancellationRequest) {
          workerManager.taskletCancelled(request.getTaskletId());
        }

        return null;
      }
    }).when(vortexRequestor).send(any(RunningTask.class), any(VortexRequest.class));

    return workerManager;
  }

  /**
   * @return a new dummy tasklet.
   */
  public Tasklet newTasklet() {
    final int id = taskletId.getAndIncrement();
    return new Tasklet(id, null, null, new VortexFuture(executor, vortexMaster, id));
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
   * @return a new dummy function.
   */
  public VortexFunction newInfiniteLoopFunction() {
    return new VortexFunction() {
      @Override
      public Serializable call(final Serializable serializable) throws Exception {
        while(true) {
          Thread.sleep(100);
          if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException();
          }
        }
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
