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
package org.apache.reef.io.network.group.impl.driver;

import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.driver.task.FailedTask;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.io.network.group.impl.GroupCommunicationMessage;
import org.apache.reef.io.network.group.impl.config.BroadcastOperatorSpec;
import org.apache.reef.io.network.group.impl.config.ReduceOperatorSpec;
import org.apache.reef.io.network.group.impl.utils.BroadcastingEventHandler;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.apache.reef.task.Task;
import org.apache.reef.wake.EStage;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.impl.SyncStage;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link CommunicationGroupDriverImpl}.
 */
public final class CommunicationGroupDriverImplTest {

  /**
   * Check that the topology builds up as expected even when the root task is added after child tasks start running.
   */
  @Test
  public void testLateRootTask() throws InterruptedException {
    final String rootTaskId = "rootTaskId";
    final String[] childTaskIds = new String[]{"childTaskId1", "childTaskId2", "childTaskId3"};
    final AtomicInteger numMsgs = new AtomicInteger(0);

    final EStage<GroupCommunicationMessage> senderStage =
        new SyncStage<>(new EventHandler<GroupCommunicationMessage>() {
          @Override
          public void onNext(final GroupCommunicationMessage msg) {
            numMsgs.getAndIncrement();
          }
        });

    final CommunicationGroupDriverImpl communicationGroupDriver = new CommunicationGroupDriverImpl(
        GroupName.class, new AvroConfigurationSerializer(), senderStage,
        new BroadcastingEventHandler<RunningTask>(), new BroadcastingEventHandler<FailedTask>(),
        new BroadcastingEventHandler<FailedEvaluator>(), new BroadcastingEventHandler<GroupCommunicationMessage>(),
        "DriverId", 4, 2);

    communicationGroupDriver
        .addBroadcast(BroadcastOperatorName.class,
            BroadcastOperatorSpec.newBuilder().setSenderId(rootTaskId).build())
        .addReduce(ReduceOperatorName.class,
            ReduceOperatorSpec.newBuilder().setReceiverId(rootTaskId).build());

    final ExecutorService pool = Executors.newFixedThreadPool(4);
    final CountDownLatch countDownLatch = new CountDownLatch(4);

    // first add child tasks and start them up
    for (int index = 0; index < 3; index++) {
      final String childId = childTaskIds[index];
      pool.submit(new Runnable() {
        @Override
        public void run() {
          final Configuration childTaskConf = TaskConfiguration.CONF
              .set(TaskConfiguration.IDENTIFIER, childId)
              .set(TaskConfiguration.TASK, DummyTask.class)
              .build();
          communicationGroupDriver.addTask(childTaskConf);
          communicationGroupDriver.runTask(childId);
          countDownLatch.countDown();
        }
      });
    }

    // next add the root task
    pool.submit(new Runnable() {
      @Override
      public void run() {
        try {
          // purposely delay the addition of the root task
          Thread.sleep(3000);
        } catch (final InterruptedException e) {
          throw new RuntimeException(e);
        }
        final Configuration rootTaskConf = TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, rootTaskId)
            .set(TaskConfiguration.TASK, DummyTask.class)
            .build();

        communicationGroupDriver.addTask(rootTaskConf);
        communicationGroupDriver.runTask(rootTaskId);
        countDownLatch.countDown();
      }
    });

    pool.shutdown();
    final boolean allThreadsFinished = countDownLatch.await(10, TimeUnit.SECONDS);
    assertTrue("all threads finished", allThreadsFinished);

    // 3 connections between 4 tasks
    // 2 messages per connection
    // 2 operations (broadcast & reduce)
    // this gives us a total of 3*2*2 = 12 messages
    assertEquals("number of messages sent from driver", 12, numMsgs.get());

  }

  /**
   * Checks that TreeTopology works correctly with the following task add sequence: child -> root -> child.
   */
  @Test
  public void testLateRootAndChildTask() throws InterruptedException {
    final String rootTaskId = "rootTaskId";
    final String[] childTaskIds = new String[]{"childTaskId1", "childTaskId2", "childTaskId3", "childTaskId4",
        "childTaskId5", "childTaskId6", "childTaskId7"};
    final AtomicInteger numMsgs = new AtomicInteger(0);

    final EStage<GroupCommunicationMessage> senderStage =
        new SyncStage<>(new EventHandler<GroupCommunicationMessage>() {
          @Override
          public void onNext(final GroupCommunicationMessage msg) {
            numMsgs.getAndIncrement();
          }
        });

    final CommunicationGroupDriverImpl communicationGroupDriver = new CommunicationGroupDriverImpl(
        GroupName.class, new AvroConfigurationSerializer(), senderStage,
        new BroadcastingEventHandler<RunningTask>(), new BroadcastingEventHandler<FailedTask>(),
        new BroadcastingEventHandler<FailedEvaluator>(), new BroadcastingEventHandler<GroupCommunicationMessage>(),
        "DriverId", 8, 2);

    communicationGroupDriver
        .addBroadcast(BroadcastOperatorName.class,
            BroadcastOperatorSpec.newBuilder().setSenderId(rootTaskId).build())
        .addReduce(ReduceOperatorName.class,
            ReduceOperatorSpec.newBuilder().setReceiverId(rootTaskId).build());

    final ExecutorService pool = Executors.newFixedThreadPool(8);
    final CountDownLatch countDownLatch = new CountDownLatch(8);

    // first add two child tasks and start them up
    for (int index = 0; index < 2; index++) {
      final String childId = childTaskIds[index];
      pool.submit(new Runnable() {
        @Override
        public void run() {
          final Configuration childTaskConf = TaskConfiguration.CONF
              .set(TaskConfiguration.IDENTIFIER, childId)
              .set(TaskConfiguration.TASK, DummyTask.class)
              .build();
          communicationGroupDriver.addTask(childTaskConf);
          communicationGroupDriver.runTask(childId);
          countDownLatch.countDown();
        }
      });
    }

    // next add the root task
    pool.submit(new Runnable() {
      @Override
      public void run() {
        try {
          // purposely delay the addition of the root task
          Thread.sleep(3000);
        } catch (final InterruptedException e) {
          throw new RuntimeException(e);
        }
        final Configuration rootTaskConf = TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, rootTaskId)
            .set(TaskConfiguration.TASK, DummyTask.class)
            .build();

        communicationGroupDriver.addTask(rootTaskConf);
        communicationGroupDriver.runTask(rootTaskId);
        countDownLatch.countDown();
      }
    });

    // then add 5 child tasks and start them up
    for (int index = 2; index < 7; index++) {
      final String childId = childTaskIds[index];
      pool.submit(new Runnable() {
        @Override
        public void run() {
          try {
            // purposely delay the addition of the child task
            Thread.sleep(6000);
          } catch (final InterruptedException e) {
            throw new RuntimeException(e);
          }
          final Configuration childTaskConf = TaskConfiguration.CONF
              .set(TaskConfiguration.IDENTIFIER, childId)
              .set(TaskConfiguration.TASK, DummyTask.class)
              .build();
          communicationGroupDriver.addTask(childTaskConf);
          communicationGroupDriver.runTask(childId);
          countDownLatch.countDown();
        }
      });
    }

    pool.shutdown();
    final boolean allThreadsFinished = countDownLatch.await(10, TimeUnit.SECONDS);
    assertTrue("all threads finished", allThreadsFinished);

    // 7 connections between 8 tasks
    // 2 messages per connection
    // 2 operations (broadcast & reduce)
    // this gives us a total of 7*2*2 = 28 messages
    assertEquals("number of messages sent from driver", 28, numMsgs.get());

  }

  private final class DummyTask implements Task {
    @Override
    public byte[] call(final byte[] memento) throws Exception {
      return null;
    }
  }

  @NamedParameter()
  private final class GroupName implements Name<String> {
  }

  @NamedParameter()
  private final class BroadcastOperatorName implements Name<String> {
  }

  @NamedParameter()
  private final class ReduceOperatorName implements Name<String> {
  }
}

