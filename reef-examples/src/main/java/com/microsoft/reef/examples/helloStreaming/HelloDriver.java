/**
 * Copyright (C) 2013 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.examples.helloStreaming;

import com.microsoft.reef.driver.task.RunningTask;
import com.microsoft.reef.driver.task.TaskConfiguration;
import com.microsoft.reef.driver.context.ContextConfiguration;
import com.microsoft.reef.driver.evaluator.AllocatedEvaluator;
import com.microsoft.reef.driver.evaluator.EvaluatorRequest;
import com.microsoft.reef.driver.evaluator.EvaluatorRequestor;
import com.microsoft.reef.driver.evaluator.EvaluatorType;
import com.microsoft.reef.examples.hello.HelloTask;
import com.microsoft.reef.examples.helloCLR.HelloCLR;
import com.microsoft.tang.ClassHierarchy;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.ConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.implementation.protobuf.ProtocolBufferClassHierarchy;
import com.microsoft.tang.proto.ClassHierarchyProto;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.time.event.StartTime;

import javax.inject.Inject;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Hashtable;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Driver code for the Hello REEF Application
 */
@Unit
public final class HelloDriver {

  private static final Logger LOG = Logger.getLogger(HelloDriver.class.getName());

  private final EvaluatorRequestor requestor;

  private int nJVMTasks = 0;  // guarded by this
  private int nCLRTasks = 2;  // guarded by this
  private Hashtable<String, String> ipAddresses = new Hashtable<String, String>();

  /**
   * Job driver constructor - instantiated via TANG.
   *
   * @param requestor evaluator requestor object used to create new evaluator containers.
   */
  @Inject
  public HelloDriver(final EvaluatorRequestor requestor) {
    this.requestor = requestor;
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
          .setSize(EvaluatorRequest.Size.SMALL)
          .build());
    }
  }

  /**
   * Handles AllocatedEvaluator: Submit an empty context and the HelloTask
   */
  final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      synchronized (HelloDriver.this) {
        if (HelloDriver.this.nJVMTasks > 0) {
          HelloDriver.this.onNextJVM(allocatedEvaluator);
          HelloDriver.this.nJVMTasks -= 1;
        } else if (HelloDriver.this.nCLRTasks > 0) {
            if ( nCLRTasks == 2) {
                //HelloDriver.this.onNextCLR(allocatedEvaluator);
                HelloDriver.this.secondSiNode(allocatedEvaluator);
            } else if (nCLRTasks == 1) {
                try {
                    String ip = ipAddresses.get("HelloStreaming2Context");

                    while (ip == null) {
                        Thread.sleep(1000);
                        ip = ipAddresses.get("HelloStreaming2Context");
                    }
                    ip = ip.substring(ip.indexOf("/") + 1, ip.indexOf(":"));
                    LOG.log(Level.INFO, "IP of the second node passed to first node: " + ip);
                    HelloDriver.this.firstSiNode(allocatedEvaluator, ip);
                }
                catch(Exception e){

                }
            }
            HelloDriver.this.nCLRTasks -= 1;
        }
      }
    }
  }

    final class TaskRunningHandler implements EventHandler<RunningTask> {
        @Override
        public void onNext(final RunningTask runningActivity) {
            String ip = runningActivity.getActiveContext().getEvaluatorDescriptor().getNodeDescriptor().getInetSocketAddress().toString();
            String id =  runningActivity.getActiveContext().getId();
            LOG.log(Level.INFO, "RunningActivity on address: " + ip + " for id " + id);
            ipAddresses.put(id, ip);
        }
    }
  /**
   * Uses the AllocatedEvaluator to launch a CLR task.
   *
   * @param allocatedEvaluator
   */
  final void onNextCLR(final AllocatedEvaluator allocatedEvaluator) {
    try {
      allocatedEvaluator.setType(EvaluatorType.CLR);
      final Configuration contextConfiguration = ContextConfiguration.CONF
          .set(ContextConfiguration.IDENTIFIER, "HelloREEFContext")
          .build();

      final Configuration taskConfiguration = getCLRTaskConfiguration("Hello_From_CLR", "Microsoft.Reef.Tasks.HelloTask");

      allocatedEvaluator.submitContextAndTask(contextConfiguration, taskConfiguration);
    } catch (final BindException ex) {
      final String message = "Unable to setup Task or Context configuration.";
      LOG.log(Level.SEVERE, message, ex);
      throw new RuntimeException(message, ex);
    }
  }

    final void firstSiNode(final AllocatedEvaluator allocatedEvaluator, String ip) {
        try {
            allocatedEvaluator.setType(EvaluatorType.CLR);
            final Configuration contextConfiguration = ContextConfiguration.CONF
                    .set(ContextConfiguration.IDENTIFIER, "HelloStreaming1Context")
                    .build();

           // final Configuration taskConfiguration = getCLRTaskConfiguration("Hello_From_Streaming1", "com.microsoft.reef.task.StreamTask1");

            final ConfigurationBuilder taskConfigurationBuilder = Tang.Factory.getTang()
                    .newConfigurationBuilder(loadClassHierarchy());

            taskConfigurationBuilder.bind("Microsoft.Reef.Tasks.TaskConfigurationOptions.Identifier", "Hello_From_Streaming1");
            taskConfigurationBuilder.bind("Microsoft.Reef.Tasks.ITask", "Microsoft.Reef.Tasks.StreamTask1");

            if (ip != null){
                taskConfigurationBuilder.bind("Microsoft.Reef.Tasks.StreamTask1.IpAddress", ip);
            }

            final Configuration taskConfiguration =  taskConfigurationBuilder.build();

            // final Configuration taskConfiguration = getCLRTaskConfiguration1("Hello_From_Streaming1");

            allocatedEvaluator.submitContextAndTask(contextConfiguration, taskConfiguration);
        } catch (final BindException ex) {
            final String message = "Unable to setup Task or Context configuration.";
            LOG.log(Level.SEVERE, message, ex);
            throw new RuntimeException(message, ex);
        }
    }

    final void secondSiNode(final AllocatedEvaluator allocatedEvaluator) {
        try {
            allocatedEvaluator.setType(EvaluatorType.CLR);
            final Configuration contextConfiguration = ContextConfiguration.CONF
                    .set(ContextConfiguration.IDENTIFIER, "HelloStreaming2Context")
                    .build();

            // final Configuration taskConfiguration = getCLRTaskConfiguration2("Hello_From_Streaming2");
            final Configuration taskConfiguration = getCLRTaskConfiguration("Hello_From_Streaming2", "Microsoft.Reef.Tasks.StreamTask2");

            allocatedEvaluator.submitContextAndTask(contextConfiguration, taskConfiguration);
        } catch (final BindException ex) {
            final String message = "Unable to setup Task or Context configuration.";
            LOG.log(Level.SEVERE, message, ex);
            throw new RuntimeException(message, ex);
        }
    }

  /**
   *
   * Uses the AllocatedEvaluator to launch a JVM task.
   *
   * @param allocatedEvaluator
   */
  final void onNextJVM(final AllocatedEvaluator allocatedEvaluator) {
    try {
      allocatedEvaluator.setType(EvaluatorType.JVM);
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
   * Makes a task configuration for the CLR Task.
   *
   * @param taskId
   * @return task configuration for the CLR Task.
   * @throws BindException
   */
  private static final Configuration getCLRTaskConfiguration(
      final String taskId, final String taskImplementationClassName) throws BindException {

    final ConfigurationBuilder taskConfigurationBuilder = Tang.Factory.getTang()
        .newConfigurationBuilder(loadClassHierarchy());

      taskConfigurationBuilder.bind("Microsoft.Reef.Tasks.TaskConfigurationOptions.Identifier", taskId);
      taskConfigurationBuilder.bind("Microsoft.Reef.Tasks.ITask", taskImplementationClassName);

    return taskConfigurationBuilder.build();
  }

  /**
   * Loads the class hierarchy.
   * @return
   */
  private static ClassHierarchy loadClassHierarchy() {
    try (final InputStream chin = new FileInputStream(HelloStreaming.CLASS_HIERARCHY_FILENAME)) {
      final ClassHierarchyProto.Node root = ClassHierarchyProto.Node.parseFrom(chin); // A
      final ClassHierarchy ch = new ProtocolBufferClassHierarchy(root);
      return ch;
    } catch (final IOException e) {
      final String message = "Unable to load class hierarchy.";
      LOG.log(Level.SEVERE, message, e);
      throw new RuntimeException(message, e);
    }
  }
}

