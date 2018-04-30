/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.reef.mock.driver;

import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.mock.driver.request.ProcessRequestInternal;
import org.apache.reef.mock.driver.runtime.MockAllocatedEvaluator;
import org.apache.reef.mock.driver.runtime.MockClock;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * basic mock tests.
 */
final class BasicMockTests {

  private MockApplication mockApplication;

  private MockRuntime mockRuntime;

  private MockClock mockClock;

  @Before
  public void initialize() throws Exception {
    final Configuration conf = MockConfiguration.CONF
        .set(MockConfiguration.ON_DRIVER_STARTED, MockApplication.StartHandler.class)
        .set(MockConfiguration.ON_DRIVER_STOP, MockApplication.StopHandler.class)
        .set(MockConfiguration.ON_CONTEXT_ACTIVE, MockApplication.ActiveContextHandler.class)
        .set(MockConfiguration.ON_CONTEXT_CLOSED, MockApplication.ContextClosedHandler.class)
        .set(MockConfiguration.ON_CONTEXT_FAILED, MockApplication.FailedContextHandler.class)
        .set(MockConfiguration.ON_EVALUATOR_ALLOCATED, MockApplication.AllocatedEvaluatorHandler.class)
        .set(MockConfiguration.ON_EVALUATOR_COMPLETED, MockApplication.CompletedEvaluatorHandler.class)
        .set(MockConfiguration.ON_EVALUATOR_FAILED, MockApplication.FailedEvaluatorHandler.class)
        .set(MockConfiguration.ON_TASK_COMPLETED, MockApplication.CompletedTaskHandler.class)
        .set(MockConfiguration.ON_TASK_FAILED, MockApplication.FailedTaskHandler.class)
        .set(MockConfiguration.ON_TASK_RUNNING, MockApplication.RunningTaskHandler.class)
        .set(MockConfiguration.ON_TASK_SUSPENDED, MockApplication.SuspendedTaskHandler.class)
        .build();

    final Injector injector = Tang.Factory.getTang().newInjector(conf);
    this.mockApplication = injector.getInstance(MockApplication.class);
    this.mockRuntime = injector.getInstance(MockRuntime.class);
    this.mockClock = injector.getInstance(MockClock.class);

    this.mockClock.run();
  }

  @Test
  public void testSuccessRequests() throws Exception {
    assertTrue("mock application received start event", this.mockApplication.isRunning());

    this.mockApplication.requestEvaluators(1);
    assertTrue("check for process event", this.mockRuntime.hasProcessRequest());
    final ProcessRequest allocateEvaluatorRequest = this.mockRuntime.getNextProcessRequest();
    assertEquals("allocate evaluator request", ProcessRequest.Type.ALLOCATE_EVALUATOR,
        allocateEvaluatorRequest.getType());
    final AllocatedEvaluator evaluator =
        ((ProcessRequestInternal<AllocatedEvaluator, Object>)allocateEvaluatorRequest)
            .getSuccessEvent();
    this.mockRuntime.succeed(allocateEvaluatorRequest);
    assertTrue("evaluator allocation succeeded",
        this.mockApplication.getAllocatedEvaluators().contains(evaluator));
    final String contextId = "foo";
    this.mockApplication.submitContext(evaluator, contextId);
    final ActiveContext rootContext = ((MockAllocatedEvaluator) evaluator).getRootContext();
    assertTrue("root context", rootContext != null);


    // submit a task
    this.mockApplication.submitTask(rootContext, "test-task");
    assertTrue("create task queued", this.mockRuntime.hasProcessRequest());
    final ProcessRequest createTaskRequest = this.mockRuntime.getNextProcessRequest();
    assertEquals("create task request", ProcessRequest.Type.CREATE_TASK,
        createTaskRequest.getType());
    final RunningTask task = (RunningTask) ((ProcessRequestInternal)createTaskRequest).getSuccessEvent();
    this.mockRuntime.succeed(createTaskRequest);
    assertTrue("task running", this.mockApplication.getRunningTasks().contains(task));

    // check task auto complete
    assertTrue("check for request", this.mockRuntime.hasProcessRequest());
    final ProcessRequestInternal completedTask =
        (ProcessRequestInternal) this.mockRuntime.getNextProcessRequest();
    assertEquals("complete task request", ProcessRequest.Type.COMPLETE_TASK,
        completedTask.getType());
    this.mockRuntime.succeed(completedTask);
    assertEquals("no running tasks", 0, this.mockApplication.getRunningTasks().size());
  }

  @Test
  public void testFailureRequests() throws Exception {
    assertTrue("mock application received start event", this.mockApplication.isRunning());

    this.mockApplication.requestEvaluators(1);
    assertTrue("check for process event", this.mockRuntime.hasProcessRequest());
    ProcessRequest allocateEvaluatorRequest = this.mockRuntime.getNextProcessRequest();
    this.mockRuntime.fail(allocateEvaluatorRequest);
    assertEquals("evaluator allocation failed", 1,
        this.mockApplication.getFailedEvaluators().size());

    this.mockApplication.requestEvaluators(1);
    allocateEvaluatorRequest = this.mockRuntime.getNextProcessRequest();
    final AllocatedEvaluator evaluator =
        (AllocatedEvaluator)((ProcessRequestInternal)allocateEvaluatorRequest).getSuccessEvent();
    this.mockRuntime.succeed(allocateEvaluatorRequest);
    this.mockApplication.submitContext(evaluator, "FOO");
    final ActiveContext rootContext = this.mockApplication
        .getContext(evaluator, "FOO");


    // submit a task
    this.mockApplication.submitTask(rootContext, "test-task");
    assertTrue("create task queued", this.mockRuntime.hasProcessRequest());
    final ProcessRequest createTaskRequest = this.mockRuntime.getNextProcessRequest();
    assertEquals("create task request", ProcessRequest.Type.CREATE_TASK,
        createTaskRequest.getType());
    this.mockRuntime.fail(createTaskRequest);
    assertEquals("task running", 1, this.mockApplication.getFailedTasks().size());
  }

  @Test
  public void testMockFailures() {
    // make sure we're running
    assertTrue("mock application received start event", this.mockApplication.isRunning());

    // allocate an evaluator and get root context
    this.mockApplication.requestEvaluators(1);
    this.mockRuntime.succeed(this.mockRuntime.getNextProcessRequest());
    final AllocatedEvaluator evaluator = this.mockRuntime.getCurrentAllocatedEvaluators().iterator().next();
    this.mockApplication.submitContext(evaluator, "FOO");
    // fail evaluator
    this.mockRuntime.fail(evaluator);
    assertEquals("evaluator failed", 1, this.mockApplication.getFailedEvaluators().size());

    // both contexts should be failed
    assertEquals("root and child contexts failed", 2,
        this.mockApplication.getFailedContext().size());
  }
}
