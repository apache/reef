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

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test whether DefaultVortexMaster correctly handles (simulated) events.
 */
public class DefaultVortexMasterTest {
  private TestUtil testUtil = new TestUtil();

  /**
   * Test handling of single tasklet execution without failure.
   */
  @Test
  public void testSingleTaskletNoFailure() throws Exception {
    final Tasklet tasklet1 = testUtil.newTasklet();
    final VortexWorkerManager vortexWorkerManager1 = testUtil.newWorker();
    final RunningWorkers runningWorkers = new RunningWorkers();
    final DefaultVortexMaster vortexMaster = new DefaultVortexMaster(runningWorkers);

    vortexMaster.workerAllocated(vortexWorkerManager1);
    vortexMaster.enqueueMockedTasklet(tasklet1);
    waitUntilTaskletIsRunning(runningWorkers, tasklet1.getId());
    vortexMaster.taskletCompleted(vortexWorkerManager1.getId(), tasklet1.getId(), null);
    assertTrue("The tasklet should have been finished", tasklet1.isCompleted());
  }

  /**
   * Test handling of single tasklet execution with a failure.
   */
  @Test
  public void testSingleTaskletFailure() throws Exception {
    final Tasklet tasklet1 = testUtil.newTasklet();
    final VortexWorkerManager vortexWorkerManager1 = testUtil.newWorker();
    final VortexWorkerManager vortexWorkerManager2 = testUtil.newWorker();
    final RunningWorkers runningWorkers = new RunningWorkers();
    final DefaultVortexMaster vortexMaster = new DefaultVortexMaster(runningWorkers);

    // Allocate and launchLocal
    vortexMaster.workerAllocated(vortexWorkerManager1);
    vortexMaster.enqueueMockedTasklet(tasklet1);
    waitUntilTaskletIsRunning(runningWorkers, tasklet1.getId());

    // Preemption!
    vortexMaster.workerPreempted(vortexWorkerManager1.getId());

    // New resource allocation and scheduling
    vortexMaster.workerAllocated(vortexWorkerManager2);
    waitUntilTaskletIsRunning(runningWorkers, tasklet1.getId());

    // Completed?
    vortexMaster.taskletCompleted(vortexWorkerManager2.getId(), tasklet1.getId(), null);
    assertTrue("The tasklet should have been finished", tasklet1.isCompleted());
  }

  /**
   * Test handling of multiple tasklet execution with failures.
   */
  @Test
  public void testMultipleTaskletsFailure() {
    // The tasklets that need to be executed
    final List<Tasklet> taskletList = new ArrayList<>();
    final RunningWorkers runningWorkers = new RunningWorkers();
    final DefaultVortexMaster vortexMaster = new DefaultVortexMaster(runningWorkers);

    // Allocate iniital evaluators (will all be preempted later...)
    final List<VortexWorkerManager> initialExecutors = new ArrayList<>();
    final int numOfWorkers = 10;
    for (int i = 0; i < numOfWorkers; i++) {
      final VortexWorkerManager vortexWorkerManager = testUtil.newWorker();
      initialExecutors.add(vortexWorkerManager);
      vortexMaster.workerAllocated(vortexWorkerManager);
    }

    // Schedule tasklets
    final int numOfTasklets = 100;
    for (int i = 0; i < numOfTasklets; i++) {
      final Tasklet tasklet = testUtil.newTasklet();
      taskletList.add(tasklet);
      vortexMaster.enqueueMockedTasklet(tasklet);
      waitUntilTaskletIsRunning(runningWorkers, tasklet.getId());
    }

    // Preempt all evaluators
    for (int i = 0; i < numOfWorkers; i++) {
      vortexMaster.workerPreempted(initialExecutors.get(i).getId());
    }

    // Allocate new evaluators
    for (int i = 0; i < numOfWorkers; i++) {
      vortexMaster.workerAllocated(testUtil.newWorker());
    }

    // For each tasklet, wait until it is re-scheduled and then complete it
    for (final Tasklet tasklet : taskletList) {
      waitUntilTaskletIsRunning(runningWorkers, tasklet.getId());
      final String executorId = runningWorkers.getWhereTaskletWasScheduledTo(tasklet.getId());
      assertNotNull("The tasklet must have been scheduled", executorId);
      vortexMaster.taskletCompleted(executorId, tasklet.getId(), null);
      assertTrue("The tasklet should have been finished", tasklet.isCompleted());
    }
  }

  /**
   * Wait until the tasklet is scheduled.
   */
  private void waitUntilTaskletIsRunning(final RunningWorkers runningWorkers, final int taskletId) {
    while (!runningWorkers.isTaskletRunning(taskletId)) {
      // Wait until the tasklet is scheduled
    }
  }
}
