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
package org.apache.reef.javabridge;

import java.util.HashMap;

public final class NativeInterop {
  public static final String CLASS_HIERARCHY_FILENAME = "clrClassHierarchy.bin";
  public static final String GLOBAL_LIBRARIES_FILENAME = "userSuppliedGlobalLibraries.txt";
  public static final String ALLOCATED_EVALUATOR_KEY = "AllocatedEvaluator";
  public static final String ACTIVE_CONTEXT_KEY = "ActiveContext";
  public static final String TASK_MESSAGE_KEY = "TaskMessage";
  public static final String FAILED_TASK_KEY = "FailedTask";
  public static final String FAILED_EVALUATOR_KEY = "FailedEvaluator";
  public static final String HTTP_SERVER_KEY = "HttpServerKey";
  public static final String COMPLETED_TASK_KEY = "CompletedTask";
  public static final String RUNNING_TASK_KEY = "RunningTask";
  public static final String SUSPENDED_TASK_KEY = "SuspendedTask";
  public static final String COMPLETED_EVALUATOR_KEY = "CompletedEvaluator";
  public static final String CLOSED_CONTEXT_KEY = "ClosedContext";
  public static final String FAILED_CONTEXT_KEY = "FailedContext";
  public static final String CONTEXT_MESSAGE_KEY = "ContextMessage";
  public static final String DRIVER_RESTART_ACTIVE_CONTEXT_KEY = "DriverRestartActiveContext";
  public static final String DRIVER_RESTART_RUNNING_TASK_KEY = "DriverRestartRunningTask";
  public static final String DRIVER_RESTART_COMPLETED_KEY = "DriverRestartCompleted";
  public static final String DRIVER_RESTART_FAILED_EVALUATOR_KEY = "DriverRestartFailedEvaluator";
  public static final HashMap<String, Integer> HANDLERS = new HashMap<String, Integer>() {
    {
      put(ALLOCATED_EVALUATOR_KEY, 1);
      put(ACTIVE_CONTEXT_KEY, 2);
      put(TASK_MESSAGE_KEY, 3);
      put(FAILED_TASK_KEY, 4);
      put(FAILED_EVALUATOR_KEY, 5);
      put(HTTP_SERVER_KEY, 6);
      put(COMPLETED_TASK_KEY, 7);
      put(RUNNING_TASK_KEY, 8);
      put(SUSPENDED_TASK_KEY, 9);
      put(COMPLETED_EVALUATOR_KEY, 10);
      put(CLOSED_CONTEXT_KEY, 11);
      put(FAILED_CONTEXT_KEY, 12);
      put(CONTEXT_MESSAGE_KEY, 13);
      put(DRIVER_RESTART_ACTIVE_CONTEXT_KEY, 14);
      put(DRIVER_RESTART_RUNNING_TASK_KEY, 15);
      put(DRIVER_RESTART_COMPLETED_KEY, 16);
      put(DRIVER_RESTART_FAILED_EVALUATOR_KEY, 17);
    }
  };

  public static final int N_HANDLERS = 18;

  public static native void loadClrAssembly(final String filePath);

  public static native void clrBufferedLog(final int level, final String message);

  public static native long[] callClrSystemOnStartHandler(
      final String dateTime,
      final String httpServerPortNumber,
      final EvaluatorRequestorBridge javaEvaluatorRequestorBridge);

  public static native void clrSystemAllocatedEvaluatorHandlerOnNext(
      final long handle,
      final AllocatedEvaluatorBridge javaEvaluatorBridge,
      final InteropLogger interopLogger
  );

  public static native void clrSystemActiveContextHandlerOnNext(
      final long handle,
      final ActiveContextBridge javaActiveContextBridge,
      final InteropLogger interopLogger
  );

  public static native void clrSystemTaskMessageHandlerOnNext(
      final long handle,
      final byte[] mesage,
      final TaskMessageBridge javaTaskMessageBridge,
      final InteropLogger interopLogger
  );

  public static native void clrSystemFailedTaskHandlerOnNext(
      final long handle,
      final FailedTaskBridge failedTaskBridge,
      final InteropLogger interopLogger
  );

  public static native void clrSystemHttpServerHandlerOnNext(
      final long handle,
      final HttpServerEventBridge httpServerEventBridge,
      final InteropLogger interopLogger
  );

  public static native void clrSystemFailedEvaluatorHandlerOnNext(
      final long handle,
      final FailedEvaluatorBridge failedEvaluatorBridge,
      final InteropLogger interopLogger
  );

  public static native void clrSystemCompletedTaskHandlerOnNext(
      final long handle,
      final CompletedTaskBridge completedTaskBridge,
      final InteropLogger interopLogger
  );

  public static native void clrSystemRunningTaskHandlerOnNext(
      final long handle,
      final RunningTaskBridge runningTaskBridge,
      final InteropLogger interopLogger
  );

  public static native void clrSystemSuspendedTaskHandlerOnNext(
      final long handle,
      final SuspendedTaskBridge suspendedTaskBridge
  );

  public static native void clrSystemCompletedEvaluatorHandlerOnNext(
      final long handle,
      final CompletedEvaluatorBridge completedEvaluatorBridge
  );

  public static native void clrSystemClosedContextHandlerOnNext(
      final long handle,
      final ClosedContextBridge closedContextBridge
  );

  public static native void clrSystemFailedContextHandlerOnNext(
      final long handle,
      final FailedContextBridge failedContextBridge
  );

  public static native void clrSystemContextMessageHandlerOnNext(
      final long handle,
      final ContextMessageBridge contextMessageBridge
  );

  public static native long[] callClrSystemOnRestartHandlerOnNext(
      final String httpServerPortNumber,
      final EvaluatorRequestorBridge javaEvaluatorRequestorBridge,
      final DriverRestartedBridge driverRestartedBridge
  );

  public static native void clrSystemDriverRestartActiveContextHandlerOnNext(
      final long handle,
      final ActiveContextBridge activeContextBridge
  );

  public static native void clrSystemDriverRestartRunningTaskHandlerOnNext(
      final long handle,
      final RunningTaskBridge runningTaskBridge
  );

  public static native void clrSystemDriverRestartCompletedHandlerOnNext(
      final long handle
  );

  public static native void clrSystemDriverRestartFailedEvaluatorHandlerOnNext(
      final long handle,
      final FailedEvaluatorBridge failedEvaluatorBridge,
      final InteropLogger interopLogger
  );

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private NativeInterop() {
  }
}
