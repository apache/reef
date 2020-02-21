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
package org.apache.reef.examples.helloCLR;

import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.CLRProcessFactory;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.examples.hello.HelloTask;
import org.apache.reef.tang.ClassHierarchy;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.ConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.implementation.protobuf.ProtocolBufferClassHierarchy;
import org.apache.reef.tang.proto.ClassHierarchyProto;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Driver code for the Hello REEF Application.
 */
@Unit
public final class HelloDriver {

  private static final Logger LOG = Logger.getLogger(HelloDriver.class.getName());

  private final EvaluatorRequestor requestor;
  private final CLRProcessFactory clrProcessFactory;

  private int nJVMTasks = 1;  // guarded by this
  private int nCLRTasks = 1;  // guarded by this


  /**
   * Job driver constructor - instantiated via TANG.
   *
   * @param requestor evaluator requestor object used to create new evaluator containers.
   */
  @Inject
  public HelloDriver(final EvaluatorRequestor requestor,
                     final CLRProcessFactory clrProcessFactory) {
    this.requestor = requestor;
    this.clrProcessFactory = clrProcessFactory;
  }

  /**
   * Makes a task configuration for the CLR Task.
   *
   * @param taskId
   * @return task configuration for the CLR Task.
   * @throws BindException
   */
  private static Configuration getCLRTaskConfiguration(final String taskId) throws BindException {
    final ConfigurationBuilder taskConfigurationBuilder = Tang.Factory.getTang()
        .newConfigurationBuilder(loadClassHierarchy());
    taskConfigurationBuilder.bind("Org.Apache.Reef.Tasks.TaskConfigurationOptions+Identifier," +
        "Org.Apache.Reef.Tasks.ITask, Version=1.0.0.0, Culture=neutral, PublicKeyToken=69c3241e6f0468ca", taskId);
    taskConfigurationBuilder.bind("Org.Apache.Reef.Tasks.ITask, Org.Apache.Reef.Tasks.ITask, Version=1.0.0.0, " +
        "Culture=neutral, PublicKeyToken=69c3241e6f0468ca", "Org.Apache.Reef.Tasks.HelloTask, " +
        "Org.Apache.Reef.Tasks.HelloTask, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null");

    return taskConfigurationBuilder.build();
  }

  /**
   * Loads the class hierarchy.
   *
   * @return
   */
  private static ClassHierarchy loadClassHierarchy() {
    // TODO[JIRA REEF-400] The file should be created by AvroClassHierarchySerializer
    try (InputStream chin = new FileInputStream(HelloCLR.CLASS_HIERARCHY_FILENAME)) {
      // TODO[JIRA REEF-400] Use AvroClassHierarchySerializer instead
      final ClassHierarchyProto.Node root = ClassHierarchyProto.Node.parseFrom(chin); // A
      final ClassHierarchy ch = new ProtocolBufferClassHierarchy(root);
      return ch;
    } catch (final IOException e) {
      final String message = "Unable to load class hierarchy.";
      LOG.log(Level.SEVERE, message, e);
      throw new RuntimeException(message, e);
    }
  }

  /**
   * Uses the AllocatedEvaluator to launch a CLR task.
   *
   * @param allocatedEvaluator
   */
  void onNextCLR(final AllocatedEvaluator allocatedEvaluator) {
    try {
      allocatedEvaluator.setProcess(clrProcessFactory.newEvaluatorProcess());
      final Configuration contextConfiguration = ContextConfiguration.CONF
          .set(ContextConfiguration.IDENTIFIER, "HelloREEFContext")
          .build();

      final Configuration taskConfiguration = getCLRTaskConfiguration("Hello_From_CLR");

      allocatedEvaluator.submitContextAndTask(contextConfiguration, taskConfiguration);
    } catch (final BindException ex) {
      final String message = "Unable to setup Task or Context configuration.";
      LOG.log(Level.SEVERE, message, ex);
      throw new RuntimeException(message, ex);
    }
  }

  /**
   * Uses the AllocatedEvaluator to launch a JVM task.
   *
   * @param allocatedEvaluator
   */
  void onNextJVM(final AllocatedEvaluator allocatedEvaluator) {
    try {
      final Configuration contextConfiguration = ContextConfiguration.CONF
          .set(ContextConfiguration.IDENTIFIER, "HelloREEFContext")
          .build();

      final Configuration taskConfiguration = TaskConfiguration.CONF
          .set(TaskConfiguration.IDENTIFIER, "HelloREEFTask")
          .set(TaskConfiguration.TASK, HelloTask.class)
          .build();

      allocatedEvaluator.submitContextAndTask(contextConfiguration, taskConfiguration);
    } catch (final BindException ex) {
      final String message = "Unable to setup Task or Context configuration.";
      LOG.log(Level.SEVERE, message, ex);
      throw new RuntimeException(message, ex);
    }
  }

  /**
   * Handles the StartTime event: Request as single Evaluator.
   */
  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      LOG.log(Level.INFO, "StartTime: ", startTime);
      HelloDriver.this.requestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(nCLRTasks + nJVMTasks)
          .setMemory(128)
          .setNumberOfCores(1)
          .build());
    }
  }

  /**
   * Handles AllocatedEvaluator: Submit an empty context and the HelloTask.
   */
  final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      synchronized (HelloDriver.this) {
        if (HelloDriver.this.nJVMTasks > 0) {
          HelloDriver.this.onNextJVM(allocatedEvaluator);
          HelloDriver.this.nJVMTasks -= 1;
        } else if (HelloDriver.this.nCLRTasks > 0) {
          HelloDriver.this.onNextCLR(allocatedEvaluator);
          HelloDriver.this.nCLRTasks -= 1;
        }
      }
    }
  }
}

