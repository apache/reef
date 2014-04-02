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
package com.microsoft.reef.examples.helloCLRBridge;

import com.microsoft.reef.driver.context.ContextConfiguration;
import com.microsoft.reef.driver.evaluator.AllocatedEvaluator;
import com.microsoft.reef.driver.evaluator.EvaluatorRequest;
import com.microsoft.reef.driver.evaluator.EvaluatorRequestor;
import com.microsoft.reef.driver.evaluator.EvaluatorType;
import com.microsoft.reef.driver.task.TaskConfiguration;
import com.microsoft.reef.examples.hello.HelloTask;
import com.microsoft.tang.ClassHierarchy;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.ConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.formats.AvroConfigurationSerializer;
import com.microsoft.tang.implementation.protobuf.ProtocolBufferClassHierarchy;
import com.microsoft.tang.proto.ClassHierarchyProto;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.time.event.StartTime;
import javabridge.AllocatedEvaluatorBridge;
import javabridge.InteropLogger;
import javabridge.NativeInterop;

import javax.inject.Inject;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Driver code for the Hello REEF Application
 */
@Unit
public final class HelloCLRBridgeDriver {

  private static final Logger LOG = Logger.getLogger(HelloCLRBridgeDriver.class.getName());

  private final EvaluatorRequestor requestor;
  private long  clrHandle;

  private int nJVMTasks = 0;  // guarded by this
  private int nCLRTasks = 1;  // guarded by this


  /**
   * Job driver constructor - instantiated via TANG.
   *
   * @param requestor evaluator requestor object used to create new evaluator containers.
   */
  @Inject
  public HelloCLRBridgeDriver(final EvaluatorRequestor requestor) {
    this.requestor = requestor;
  }

  /**
   * Handles the StartTime event: Request as single Evaluator.
   */
  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      LOG.log(Level.INFO, "StartTime: ", startTime);
        clrHandle = NativeInterop.CallClrSystemOnStartHandler(startTime.toString());
        HelloCLRBridgeDriver.this.requestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(nCLRTasks + nJVMTasks)
          .setMemory(128)
          .build());
    }
  }

  /**
   * Handles AllocatedEvaluator: Submit an empty context and the HelloTask
   */
  final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      synchronized (HelloCLRBridgeDriver.this) {
        if (HelloCLRBridgeDriver.this.nJVMTasks > 0) {
            HelloCLRBridgeDriver.this.onNextJVM(allocatedEvaluator);
            HelloCLRBridgeDriver.this.nJVMTasks -= 1;
        } else if (HelloCLRBridgeDriver.this.nCLRTasks > 0) {
            HelloCLRBridgeDriver.this.onNextCLR(allocatedEvaluator);
            HelloCLRBridgeDriver.this.nCLRTasks -= 1;
        }
      }
    }
  }

  /**
   * Uses the AllocatedEvaluator to launch a CLR task.
   *
   * @param allocatedEvaluator
   */
  final void onNextCLR(final AllocatedEvaluator allocatedEvaluator) {
    try {
      InteropLogger interopLogger = new InteropLogger();
      allocatedEvaluator.setType(EvaluatorType.CLR);
      AllocatedEvaluatorBridge allocatedEvaluatorBridge = new AllocatedEvaluatorBridge(allocatedEvaluator);
      NativeInterop.ClrSystemAllocatedEvaluatorHandlerOnNext(clrHandle, allocatedEvaluatorBridge,interopLogger);
    } catch (final Exception ex) {
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
}

