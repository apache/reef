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

import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.util.Optional;
import org.apache.reef.vortex.api.FutureCallback;
import org.apache.reef.vortex.api.VortexFunction;
import org.apache.reef.vortex.api.VortexFuture;
import org.apache.reef.vortex.protocol.workertomaster.TaskletFailureReport;
import org.apache.reef.vortex.protocol.workertomaster.WorkerToMasterReport;
import org.apache.reef.vortex.protocol.workertomaster.TaskletResultReport;
import org.apache.reef.vortex.protocol.workertomaster.WorkerToMasterReports;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

/**
 * Test whether DefaultVortexMaster correctly handles (simulated) events.
 */
public class DefaultVortexMasterTest {
  private static final Object EMPTY_RESULT = null;
  private static final int INTEGER_RESULT = 1;
  private TestUtil testUtil = new TestUtil();

  /**
   * Test handling of single tasklet execution without failure.
   */
  @Test(timeout = 10000)
  public void testSingleTaskletNoFailure() throws Exception {
    final VortexFunction vortexFunction = testUtil.newIntegerFunction();
    final VortexWorkerManager vortexWorkerManager1 = testUtil.newWorker();
    final RunningWorkers runningWorkers = new RunningWorkers(new RandomSchedulingPolicy(),
        testUtil.newAggregateFunctionRepository());
    final PendingTasklets pendingTasklets = new PendingTasklets();
    final DefaultVortexMaster vortexMaster = new DefaultVortexMaster(runningWorkers, pendingTasklets,
        testUtil.newAggregateFunctionRepository(), 5);

    final AtomicBoolean callbackReceived = new AtomicBoolean(false);
    final CountDownLatch latch = new CountDownLatch(1);

    vortexMaster.workerAllocated(vortexWorkerManager1);

    final FutureCallback<Integer> testCallbackHandler = new FutureCallback<Integer>() {
      @Override
      public void onSuccess(final Integer integer) {
        callbackReceived.set(true);
        latch.countDown();
      }

      @Override
      public void onFailure(final Throwable throwable) {
        throw new RuntimeException("Did not expect exception in test.", throwable);
      }
    };

    final VortexFuture future = vortexMaster.enqueueTasklet(vortexFunction, null, Optional.of(testCallbackHandler));

    final ArrayList<Integer> taskletIds = launchTasklets(runningWorkers, pendingTasklets, 1);
    for (final int taskletId : taskletIds) {
      final WorkerToMasterReport workerToMasterReport = new TaskletResultReport(taskletId, INTEGER_RESULT);
      vortexMaster.workerReported(
          vortexWorkerManager1.getId(), new WorkerToMasterReports(Collections.singletonList(workerToMasterReport)));
    }

    assertTrue("The VortexFuture should be done", future.isDone());
    latch.await();
    assertTrue("Callback should have been received", callbackReceived.get());
  }

  /**
   * Test handling of single tasklet execution with a failure.
   */
  @Test(timeout = 10000)
  public void testSingleTaskletFailure() throws Exception {
    final VortexFunction vortexFunction = testUtil.newFunction();
    final VortexWorkerManager vortexWorkerManager1 = testUtil.newWorker();
    final VortexWorkerManager vortexWorkerManager2 = testUtil.newWorker();
    final RunningWorkers runningWorkers = new RunningWorkers(new RandomSchedulingPolicy(),
        testUtil.newAggregateFunctionRepository());
    final PendingTasklets pendingTasklets = new PendingTasklets();
    final DefaultVortexMaster vortexMaster = new DefaultVortexMaster(runningWorkers, pendingTasklets,
        testUtil.newAggregateFunctionRepository(), 5);

    // Allocate worker & tasklet and schedule
    vortexMaster.workerAllocated(vortexWorkerManager1);
    final VortexFuture future = vortexMaster.enqueueTasklet(vortexFunction, null,
        Optional.<FutureCallback<Integer>>empty());
    final ArrayList<Integer> taskletIds1 = launchTasklets(runningWorkers, pendingTasklets, 1);

    // Preemption!
    vortexMaster.workerPreempted(vortexWorkerManager1.getId());
    assertFalse("The VortexFuture should not be done", future.isDone());

    // New resource allocation and scheduling
    vortexMaster.workerAllocated(vortexWorkerManager2);
    final ArrayList<Integer> taskletIds2 = launchTasklets(runningWorkers, pendingTasklets, 1);
    assertEquals("Both lists need to contain the same single tasklet id", taskletIds1, taskletIds2);


    // Completed?
    for (final int taskletId : taskletIds2) {
      final WorkerToMasterReport workerToMasterReport = new TaskletResultReport(taskletId, EMPTY_RESULT);
      vortexMaster.workerReported(
          vortexWorkerManager2.getId(), new WorkerToMasterReports(Collections.singletonList(workerToMasterReport)));
    }
    assertTrue("The VortexFuture should be done", future.isDone());
  }

  /**
   * Test handling of multiple tasklet execution with failures.
   */
  @Test(timeout = 10000)
  public void testMultipleTaskletsFailure() throws Exception {
    // The tasklets that need to be executed
    final ArrayList<VortexFuture> vortexFutures = new ArrayList<>();
    final RunningWorkers runningWorkers = new RunningWorkers(new RandomSchedulingPolicy(),
        testUtil.newAggregateFunctionRepository());
    final PendingTasklets pendingTasklets = new PendingTasklets();
    final DefaultVortexMaster vortexMaster = new DefaultVortexMaster(runningWorkers, pendingTasklets,
        testUtil.newAggregateFunctionRepository(), 5);

    // Allocate iniital evaluators (will all be preempted later...)
    final List<VortexWorkerManager> initialWorkers = new ArrayList<>();
    final int numOfWorkers = 100;
    for (int i = 0; i < numOfWorkers; i++) {
      final VortexWorkerManager vortexWorkerManager = testUtil.newWorker();
      initialWorkers.add(vortexWorkerManager);
      vortexMaster.workerAllocated(vortexWorkerManager);
    }

    // Schedule tasklets
    final int numOfTasklets = 100;
    for (int i = 0; i < numOfTasklets; i++) {
      vortexFutures.add(vortexMaster.enqueueTasklet(testUtil.newFunction(), null,
          Optional.<FutureCallback<Void>>empty()));
    }
    final ArrayList<Integer> taskletIds1 = launchTasklets(runningWorkers, pendingTasklets, numOfTasklets);

    // Preempt all evaluators
    for (int i = 0; i < numOfWorkers; i++) {
      vortexMaster.workerPreempted(initialWorkers.get(i).getId());
    }

    // Allocate new evaluators and reschedule
    for (int i = 0; i < numOfWorkers; i++) {
      vortexMaster.workerAllocated(testUtil.newWorker());
    }
    final ArrayList<Integer> taskletIds2 = launchTasklets(runningWorkers, pendingTasklets, numOfTasklets);
    assertEquals("Must contain same tasklet ids", new HashSet<>(taskletIds1), new HashSet<>(taskletIds2));

    // Completed?
    for (final int taskletId : taskletIds2) {
      final String workerId = runningWorkers.getWhereTaskletWasScheduledTo(taskletId);
      assertNotNull("The tasklet must have been scheduled", workerId);
      final WorkerToMasterReport workerToMasterReport = new TaskletResultReport(taskletId, EMPTY_RESULT);
      vortexMaster.workerReported(
          workerId, new WorkerToMasterReports(Collections.singletonList(workerToMasterReport)));
    }
    for (final VortexFuture vortexFuture : vortexFutures) {
      assertTrue("The VortexFuture should be done", vortexFuture.isDone());
    }
  }

  /**
   * Test handling of single tasklet execution with a failure.
   */
  @Test(timeout = 10000)
  public void testTaskletThrowException() throws Exception {
    final VortexFunction vortexFunction = testUtil.newIntegerFunction();
    final VortexWorkerManager vortexWorkerManager1 = testUtil.newWorker();
    final RunningWorkers runningWorkers = new RunningWorkers(new RandomSchedulingPolicy(),
        testUtil.newAggregateFunctionRepository());
    final PendingTasklets pendingTasklets = new PendingTasklets();
    final DefaultVortexMaster vortexMaster = new DefaultVortexMaster(runningWorkers, pendingTasklets,
        testUtil.newAggregateFunctionRepository(), 5);

    final AtomicBoolean callbackReceived = new AtomicBoolean(false);
    final CountDownLatch latch = new CountDownLatch(1);

    vortexMaster.workerAllocated(vortexWorkerManager1);

    final FutureCallback<Integer> testCallbackHandler = new FutureCallback<Integer>() {
      @Override
      public void onSuccess(final Integer integer) {
        throw new RuntimeException("Did not expect success in test.");
      }

      @Override
      public void onFailure(final Throwable throwable) {
        callbackReceived.set(true);
        latch.countDown();
      }
    };

    final VortexFuture future = vortexMaster.enqueueTasklet(vortexFunction, null, Optional.of(testCallbackHandler));

    final ArrayList<Integer> taskletIds = launchTasklets(runningWorkers, pendingTasklets, 1);

    for (final int taskletId : taskletIds) {
      final WorkerToMasterReport workerToMasterReport =
          new TaskletFailureReport(taskletId, new RuntimeException("Test exception."));
      vortexMaster.workerReported(
          vortexWorkerManager1.getId(), new WorkerToMasterReports(Collections.singletonList(workerToMasterReport)));
    }

    assertTrue("The VortexFuture should be done", future.isDone());
    latch.await();
    assertTrue("Callback should have been received", callbackReceived.get());
  }

  /**
   * Test handling of single tasklet execution with a cancellation after launch.
   */
  @Test(timeout = 10000)
  public void testSingleTaskletCancellation() throws Exception {
    final RunningWorkers runningWorkers = new RunningWorkers(new RandomSchedulingPolicy(),
        testUtil.newAggregateFunctionRepository());
    final PendingTasklets pendingTasklets = new PendingTasklets();
    final VortexFuture future = createTaskletCancellationFuture(runningWorkers, pendingTasklets);
    launchTasklets(runningWorkers, pendingTasklets, 1);

    assertTrue(future.cancel(true));
    assertTrue("The VortexFuture should be cancelled.", future.isCancelled());
    assertTrue("The VortexFuture should be done", future.isDone());
  }

  /**
   * Test handling of single tasklet execution with a cancellation before launch.
   */
  @Test(timeout = 10000)
  public void testSingleTaskletCancellationBeforeLaunch() throws Exception {

    final RunningWorkers runningWorkers = new RunningWorkers(new RandomSchedulingPolicy(),
        testUtil.newAggregateFunctionRepository());
    final PendingTasklets pendingTasklets = new PendingTasklets();
    final VortexFuture future = createTaskletCancellationFuture(runningWorkers, pendingTasklets);

    try {
      future.cancel(true, 100, TimeUnit.MILLISECONDS);
      fail();
    } catch (final TimeoutException expected) {
      // TimeoutException is expected.
    }

    launchTasklets(runningWorkers, pendingTasklets, 1);
    assertTrue(future.cancel(true));
    assertTrue("The VortexFuture should be cancelled.", future.isCancelled());
    assertTrue("The VortexFuture should be done", future.isDone());
  }

  private VortexFuture createTaskletCancellationFuture(
      final RunningWorkers runningWorkers, final PendingTasklets pendingTasklets) throws InjectionException {
    final VortexFunction vortexFunction = testUtil.newInfiniteLoopFunction();
    final DefaultVortexMaster vortexMaster = new DefaultVortexMaster(
        runningWorkers, pendingTasklets,
        testUtil.newAggregateFunctionRepository(), 5);
    final VortexWorkerManager vortexWorkerManager1 = testUtil.newWorker(vortexMaster);


    // Allocate worker & tasklet and schedule
    vortexMaster.workerAllocated(vortexWorkerManager1);
    return vortexMaster.enqueueTasklet(vortexFunction, null, Optional.<FutureCallback<Integer>>empty());
  }

  /**
   * Launch specified number of tasklets as a substitute for PendingTaskletLauncher.
   * @return ids of launched tasklets
   */
  private ArrayList<Integer> launchTasklets(final RunningWorkers runningWorkers,
                                            final PendingTasklets pendingTasklets,
                                            final int numOfTasklets) throws InterruptedException {
    final ArrayList<Integer> taskletIds = new ArrayList<>();
    for (int i = 0; i < numOfTasklets; i++) {
      final Tasklet tasklet = pendingTasklets.takeFirst(); // blocks when no tasklet exists
      assertNotNull("Tasklet should exist in the pending queue", tasklet);
      runningWorkers.launchTasklet(tasklet); // blocks when no resource exists
      taskletIds.add(tasklet.getId());
    }
    return taskletIds;
  }
}