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
import javabridge.EvaluatorRequstorBridge;
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

  private final EvaluatorRequstorBridge requstorBridge;
  private long  allocatedEvaluatorHandler;
  private long  evaloatorReqeustorHandler;

  private int nCLRTasks = 0;

  /**
   * Job driver constructor - instantiated via TANG.
   *
   * @param requestor evaluator requestor object used to create new evaluator containers.
   */
  @Inject
  public HelloCLRBridgeDriver(final EvaluatorRequestor requestor) {
     requstorBridge = new EvaluatorRequstorBridge(requestor);
  }

  /**
   * Handles the StartTime event: Request as single Evaluator.
   */
  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
        synchronized (HelloCLRBridgeDriver.this)
        {
            InteropLogger interopLogger = new InteropLogger();
            LOG.log(Level.INFO, "StartTime: ", startTime);
            long[] handlers = NativeInterop.CallClrSystemOnStartHandler(startTime.toString());
            if (handlers != null)
            {
                if(handlers.length > 0)
                {
                    allocatedEvaluatorHandler = handlers[0];
                }
                if (handlers.length > 2)
                {
                    evaloatorReqeustorHandler = handlers[2];
                }
            }
            if(evaloatorReqeustorHandler == 0)
            {
                throw new RuntimeException("Evaluator Requestor Handler not initialized by CLR.");
            }
            NativeInterop.ClrSystemEvaluatorRequstorHandlerOnNext(evaloatorReqeustorHandler, requstorBridge, interopLogger);

            // get the evaluator numbers set by CLR handler
            nCLRTasks =  requstorBridge.getEvaluaotrNumber();
            LOG.log(Level.INFO, "evaluator requested: " + nCLRTasks);
        }
    }
  }

  /**
   * Handles AllocatedEvaluator: Submit an empty context and the HelloTask
   */
  final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      synchronized (HelloCLRBridgeDriver.this) {
          LOG.log(Level.INFO, "evaluator outstanding: " + nCLRTasks);
          if (HelloCLRBridgeDriver.this.nCLRTasks > 0) {
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
    if(allocatedEvaluatorHandler == 0)
    {
        throw new RuntimeException("Allocated Evaluator Handler not initialized by CLR.");
    }
    InteropLogger interopLogger = new InteropLogger();
    allocatedEvaluator.setType(EvaluatorType.CLR);
    AllocatedEvaluatorBridge allocatedEvaluatorBridge = new AllocatedEvaluatorBridge(allocatedEvaluator);
    NativeInterop.ClrSystemAllocatedEvaluatorHandlerOnNext(allocatedEvaluatorHandler, allocatedEvaluatorBridge,interopLogger);
  }
}

