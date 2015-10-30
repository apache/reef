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
package org.apache.reef.tests.group;

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.task.CompletedTask;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.io.network.group.api.driver.CommunicationGroupDriver;
import org.apache.reef.io.network.group.api.driver.GroupCommDriver;
import org.apache.reef.io.network.group.impl.config.BroadcastOperatorSpec;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Driver used for testing multiple communication groups.
 */
@DriverSide
@Unit
public final class MultipleCommGroupsDriver {
  private static final Logger LOG = Logger.getLogger(MultipleCommGroupsDriver.class.getName());

  private final EvaluatorRequestor requestor;
  private final GroupCommDriver groupCommDriver;

  private final String[][] taskIds;
  private final AtomicInteger[] taskCounter;
  private final List<CommunicationGroupDriver> commGroupDriverList;
  private final List<ActiveContext> activeContextsToBeHandled;

  @Inject
  private MultipleCommGroupsDriver(final EvaluatorRequestor requestor,
                                   final GroupCommDriver groupCommDriver) {
    this.requestor = requestor;
    this.groupCommDriver = groupCommDriver;
    taskIds = new String[][]{
        {"MasterTask-1", "SlaveTask-1-1", "SlaveTask-1-2", "SlaveTask-1-3"},
        {"MasterTask-2", "SlaveTask-2-1"}
    };
    taskCounter = new AtomicInteger[]{new AtomicInteger(0), new AtomicInteger(0)};
    commGroupDriverList = new ArrayList<>(2);
    activeContextsToBeHandled = new ArrayList<>(2);
    initializeCommGroups();
  }

  private void initializeCommGroups() {
    commGroupDriverList.add(groupCommDriver.newCommunicationGroup(Group1.class, 4));
    commGroupDriverList.add(groupCommDriver.newCommunicationGroup(Group2.class, 2));
    commGroupDriverList.get(0).addBroadcast(BroadcastOperatorName.class,
        BroadcastOperatorSpec.newBuilder()
            .setSenderId(taskIds[0][0])
            .setDataCodecClass(SerializableCodec.class)
            .build());
    commGroupDriverList.get(1).addBroadcast(BroadcastOperatorName.class,
        BroadcastOperatorSpec.newBuilder()
            .setSenderId(taskIds[1][0])
            .setDataCodecClass(SerializableCodec.class)
            .build());
  }

  final class StartHandler implements EventHandler<StartTime> {

    @Override
    public void onNext(final StartTime startTime) {
      requestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(4)
          .setMemory(128)
          .build());
    }
  }

  final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {

    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      LOG.log(Level.INFO, "Evaluator allocated {0}", allocatedEvaluator);
      allocatedEvaluator.submitContextAndService(
          groupCommDriver.getContextConfiguration(), groupCommDriver.getServiceConfiguration());
    }
  }

  final class ContextActiveHandler implements EventHandler<ActiveContext> {
    private final AtomicInteger contextCounter = new AtomicInteger(0);

    @Override
    public void onNext(final ActiveContext activeContext) {
      final int count = contextCounter.getAndIncrement();

      if (count <= 1) {
        LOG.log(Level.INFO, "{0} will be handled after tasks in Group1 started", activeContext);
        activeContextsToBeHandled.add(activeContext);
      } else {
        // Add task to Group1
        submitTask(activeContext, 0);
      }
    }
  }

  final class TaskRunningHandler implements EventHandler<RunningTask> {
    private final AtomicInteger runningTaskCounter = new AtomicInteger(0);

    @Override
    public void onNext(final RunningTask runningTask) {
      LOG.log(Level.INFO, "{0} has started", runningTask);
      final int count = runningTaskCounter.getAndIncrement();
      // After two tasks has started, submit tasks to the active contexts in activeContextsToBeHandled
      if (count == 1) {
        for (final ActiveContext activeContext : activeContextsToBeHandled) {
          // Add task to Group2
          submitTask(activeContext, 1);
        }
      }
    }
  }

  private void submitTask(final ActiveContext activeContext, final int groupIndex) {
    final String taskId = taskIds[groupIndex][taskCounter[groupIndex].getAndIncrement()];
    LOG.log(Level.INFO, "Got active context {0}. Submit {1}", new Object[]{activeContext, taskId});
    final Configuration partialTaskConf;
    if (taskId.equals(taskIds[groupIndex][0])) {
      partialTaskConf = TaskConfiguration.CONF
          .set(TaskConfiguration.IDENTIFIER, taskId)
          .set(TaskConfiguration.TASK, MasterTask.class)
          .build();
    } else {
      partialTaskConf = TaskConfiguration.CONF
          .set(TaskConfiguration.IDENTIFIER, taskId)
          .set(TaskConfiguration.TASK, SlaveTask.class)
          .build();
    }
    commGroupDriverList.get(groupIndex).addTask(partialTaskConf);
    activeContext.submitTask(groupCommDriver.getTaskConfiguration(partialTaskConf));
  }

  final class TaskCompletedHandler implements EventHandler<CompletedTask> {
    private final AtomicInteger completedTaskCounter = new AtomicInteger(0);

    @Override
    public void onNext(final CompletedTask completedTask) {
      final int count = completedTaskCounter.getAndIncrement();
      LOG.log(Level.INFO, "{0} has completed.", completedTask);
      if (count <= 1) {
        // Add task to Group1
        submitTask(completedTask.getActiveContext(), 0);
      } else {
        completedTask.getActiveContext().close();
      }
    }
  }

  @NamedParameter()
  final class Group1 implements Name<String> {
  }

  @NamedParameter()
  final class Group2 implements Name<String> {
  }

  @NamedParameter()
  final class BroadcastOperatorName implements Name<String> {
  }
}
