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

import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.vortex.common.AggregateFunctionRepository;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test Possible Race Conditions.
 */
public class RunningWorkersTest {
  private final TestUtil testUtil = new TestUtil();
  private final TestUtil.TestSchedulingPolicy schedulingPolicy = testUtil.newSchedulingPolicy();
  private final RunningWorkers runningWorkers;

  public RunningWorkersTest() throws InjectionException {
    runningWorkers = new RunningWorkers(
        schedulingPolicy, Tang.Factory.getTang().newInjector().getInstance(AggregateFunctionRepository.class));
  }

  /**
   * Test executor preemption -> executor allocation.
   * Possible scenario: RM preempts Evaluator before the Evaluator sends its first heartbeat to Driver.
   */
  @Test
  public void removeExecutorAndAddExecutor() throws Exception {
    final VortexWorkerManager vortexWorkerManager = testUtil.newWorker();
    assertFalse("Must be no running tasklets", runningWorkers.removeWorker(vortexWorkerManager.getId()).isPresent());
    runningWorkers.addWorker(vortexWorkerManager);
    assertFalse("Executor should not be running", runningWorkers.isWorkerRunning(vortexWorkerManager.getId()));
  }

  /**
   * Test executor allocation -> tasklet launch -> executor preemption -> tasklet complete
   * Possible scenario: Tasklet completion message from Evaluator comes in slowly, after preemption message from RM.
   */
  @Test(timeout = 10000)
  public void removeExecutorAndCompleteTasklet() throws Exception {
    final VortexWorkerManager vortexWorkerManager = testUtil.newWorker();
    final Tasklet tasklet = testUtil.newTasklet();
    runningWorkers.addWorker(vortexWorkerManager);
    runningWorkers.launchTasklet(tasklet); // blocks when no worker exists
    final Collection<Tasklet> tasklets = runningWorkers.removeWorker(vortexWorkerManager.getId()).get();
    assertEquals("Only 1 Tasklet must have been running", 1, tasklets.size());
    assertTrue("This Tasklet must have been running", tasklets.contains(tasklet));
    final List<Integer> taskletIds = new ArrayList<>();
    for (final Tasklet taskletIter : tasklets) {
      taskletIds.add(taskletIter.getId());
    }

    runningWorkers.doneTasklets(vortexWorkerManager.getId(), taskletIds);
    assertFalse("Tasklet must not have been completed", schedulingPolicy.taskletIsDone(tasklet.getId()));
  }
}