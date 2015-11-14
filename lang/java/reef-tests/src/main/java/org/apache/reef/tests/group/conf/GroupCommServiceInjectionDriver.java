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
package org.apache.reef.tests.group.conf;

import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.io.network.group.api.driver.CommunicationGroupDriver;
import org.apache.reef.io.network.group.api.driver.GroupCommDriver;
import org.apache.reef.io.network.group.impl.config.BroadcastOperatorSpec;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Driver code for the GroupCommServiceInjection test.
 * Spawns two evaluators that uses group communication to send and receive a single integer.
 * To make {@link GroupCommServiceInjectionCodec} instantiate correctly, this binds a Tang configuration that
 * contains a value for {@link GroupCommServiceInjectionParameter} with the group comm service.
 */
@Unit
final class GroupCommServiceInjectionDriver {
  static final Integer SEND_INTEGER = 5;
  static final Integer OFFSET = 10;

  private final EvaluatorRequestor evaluatorRequestor;
  private final GroupCommDriver groupCommDriver;
  private final CommunicationGroupDriver commGroupDriver;

  @Inject
  private GroupCommServiceInjectionDriver(final EvaluatorRequestor evaluatorRequestor,
                                          final GroupCommDriver groupCommDriver) {
    this.evaluatorRequestor = evaluatorRequestor;
    this.groupCommDriver = groupCommDriver;
    this.commGroupDriver =
        groupCommDriver.newCommunicationGroup(GroupCommServiceInjectionGroupName.class, 2);

    this.commGroupDriver
        .addBroadcast(GroupCommServiceInjectionBroadcast.class,
            BroadcastOperatorSpec.newBuilder()
                .setSenderId(GroupCommServiceInjectionMasterTask.TASK_ID)
                .setDataCodecClass(GroupCommServiceInjectionCodec.class)
                .build())
        .finalise();
  }

  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      evaluatorRequestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(2)
          .setMemory(128)
          .build());
    }
  }

  final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      allocatedEvaluator.submitContextAndService(
          groupCommDriver.getContextConfiguration(), groupCommDriver.getServiceConfiguration());
    }
  }

  final class ContextActiveHandler implements EventHandler<ActiveContext> {
    private final AtomicBoolean masterTaskSubmitted = new AtomicBoolean(false);

    @Override
    public void onNext(final ActiveContext activeContext) {
      final Configuration paramConf = Tang.Factory.getTang().newConfigurationBuilder()
          .bindNamedParameter(GroupCommServiceInjectionParameter.class, Integer.toString(OFFSET))
          .build();

      if (masterTaskSubmitted.compareAndSet(false, true)) {
        final Configuration masterTaskPartialConf = TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, GroupCommServiceInjectionMasterTask.TASK_ID)
            .set(TaskConfiguration.TASK, GroupCommServiceInjectionMasterTask.class)
            .build();
        commGroupDriver.addTask(masterTaskPartialConf);

        final Configuration masterTaskFinalConf = groupCommDriver.getTaskConfiguration(
            Configurations.merge(paramConf, masterTaskPartialConf));
        activeContext.submitTask(masterTaskFinalConf);


      } else {
        final Configuration slaveTaskPartialConf = TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, GroupCommServiceInjectionSlaveTask.TASK_ID)
            .set(TaskConfiguration.TASK, GroupCommServiceInjectionSlaveTask.class)
            .build();
        commGroupDriver.addTask(slaveTaskPartialConf);

        final Configuration slaveTaskFinalConf = groupCommDriver.getTaskConfiguration(
            Configurations.merge(paramConf, slaveTaskPartialConf));
        activeContext.submitTask(slaveTaskFinalConf);

      }
    }
  }

  @NamedParameter(doc = "Named parameter to be used by GroupCommServiceInjectionCodec")
  final class GroupCommServiceInjectionParameter implements Name<Integer> {
  }

  @NamedParameter(doc = "Operation name for GroupCommServiceInjection test")
  final class GroupCommServiceInjectionBroadcast implements Name<String> {
  }

  @NamedParameter(doc = "GC group name used for GroupCommServiceInjection test")
  final class GroupCommServiceInjectionGroupName implements Name<String> {
  }
}
