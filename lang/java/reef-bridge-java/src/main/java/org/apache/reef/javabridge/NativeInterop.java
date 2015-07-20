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
  public static final String DRIVER_RESTART_KEY = "DriverRestart";
  public static final String DRIVER_RESTART_ACTIVE_CONTEXT_KEY = "DriverRestartActiveContext";
  public static final String DRIVER_RESTART_RUNNING_TASK_KEY = "DriverRestartRunningTask";
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
      put(DRIVER_RESTART_KEY, 14);
      put(DRIVER_RESTART_ACTIVE_CONTEXT_KEY, 15);
      put(DRIVER_RESTART_RUNNING_TASK_KEY, 16);
    }
  };

  public static final int N_HANDLERS = 17;

  public static native void loadClrAssembly(String filePath);

  public static native void clrBufferedLog(int level, String message);

  public static native long[] callClrSystemOnStartHandler(
      String dateTime,
      String httpServerPortNumber,
      EvaluatorRequestorBridge javaEvaluatorRequestorBridge);

  public static native void clrSystemAllocatedEvaluatorHandlerOnNext(
      long handle,
      AllocatedEvaluatorBridge javaEvaluatorBridge,
      InteropLogger interopLogger
  );

  public static native void clrSystemActiveContextHandlerOnNext(
      long handle,
      ActiveContextBridge javaActiveContextBridge,
      InteropLogger interopLogger
  );

  public static native void clrSystemTaskMessageHandlerOnNext(
      long handle,
      byte[] mesage,
      TaskMessageBridge javaTaskMessageBridge,
      InteropLogger interopLogger
  );

  public static native void clrSystemFailedTaskHandlerOnNext(
      long handle,
      FailedTaskBridge failedTaskBridge,
      InteropLogger interopLogger
  );

  public static native void clrSystemHttpServerHandlerOnNext(
      long handle,
      HttpServerEventBridge httpServerEventBridge,
      InteropLogger interopLogger
  );

  public static native void clrSystemFailedEvaluatorHandlerOnNext(
      long handle,
      FailedEvaluatorBridge failedEvaluatorBridge,
      InteropLogger interopLogger
  );

  public static native void clrSystemCompletedTaskHandlerOnNext(
      long handle,
      CompletedTaskBridge completedTaskBridge,
      InteropLogger interopLogger
  );

  public static native void clrSystemRunningTaskHandlerOnNext(
      long handle,
      RunningTaskBridge runningTaskBridge,
      InteropLogger interopLogger
  );

  public static native void clrSystemSuspendedTaskHandlerOnNext(
      long handle,
      SuspendedTaskBridge suspendedTaskBridge
  );

  public static native void clrSystemCompletedEvaluatorHandlerOnNext(
      long handle,
      CompletedEvaluatorBridge completedEvaluatorBridge
  );

  public static native void clrSystemClosedContextHandlerOnNext(
      long handle,
      ClosedContextBridge closedContextBridge
  );

  public static native void clrSystemFailedContextHandlerOnNext(
      long handle,
      FailedContextBridge failedContextBridge
  );

  public static native void clrSystemContextMessageHandlerOnNext(
      long handle,
      ContextMessageBridge contextMessageBridge
  );

  public static native void clrSystemDriverRestartHandlerOnNext(
      long handle
  );

  public static native void clrSystemDriverRestartActiveContextHandlerOnNext(
      long handle,
      ActiveContextBridge activeContextBridge
  );

  public static native void clrSystemDriverRestartRunningTaskHandlerOnNext(
      long handle,
      RunningTaskBridge runningTaskBridge
  );

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private NativeInterop() {
  }
}
