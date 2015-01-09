/**
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

public class NativeInterop {
  public static final String CLASS_HIERARCHY_FILENAME = "clrClassHierarchy.bin";
  public static final String GLOBAL_LIBRARIES_FILENAME = "userSuppliedGlobalLibraries.txt";
  public static final String EvaluatorRequestorKey = "EvaluatorRequestor";
  public static final String AllocatedEvaluatorKey = "AllocatedEvaluator";
  public static final String ActiveContextKey = "ActiveContext";
  public static final String TaskMessageKey = "TaskMessage";
  public static final String FailedTaskKey = "FailedTask";
  public static final String FailedEvaluatorKey = "FailedEvaluator";
  public static final String HttpServerKey = "HttpServerKey";
  public static final String CompletedTaskKey = "CompletedTask";
  public static final String RunningTaskKey = "RunningTask";
  public static final String SuspendedTaskKey = "SuspendedTask";
  public static final String CompletedEvaluatorKey = "CompletedEvaluator";
  public static final String ClosedContextKey = "ClosedContext";
  public static final String FailedContextKey = "FailedContext";
  public static final String ContextMessageKey = "ContextMessage";
  public static final String DriverRestartKey = "DriverRestart";
  public static final String DriverRestartActiveContextKey = "DriverRestartActiveContext";
  public static final String DriverRestartRunningTaskKey = "DriverRestartRunningTask";
  public static final HashMap<String, Integer> Handlers = new HashMap<String, Integer>() {
    {
      put(EvaluatorRequestorKey, 0);
      put(AllocatedEvaluatorKey, 1);
      put(ActiveContextKey, 2);
      put(TaskMessageKey, 3);
      put(FailedTaskKey, 4);
      put(FailedEvaluatorKey, 5);
      put(HttpServerKey, 6);
      put(CompletedTaskKey, 7);
      put(RunningTaskKey, 8);
      put(SuspendedTaskKey, 9);
      put(CompletedEvaluatorKey, 10);
      put(ClosedContextKey, 11);
      put(FailedContextKey, 12);
      put(ContextMessageKey, 13);
      put(DriverRestartKey, 14);
      put(DriverRestartActiveContextKey, 15);
      put(DriverRestartRunningTaskKey, 16);
    }
  };

  public static final int nHandlers = 17;

  public static native void loadClrAssembly(String filePath);

  public static native void ClrBufferedLog(int level, String message);

  public static native long[] CallClrSystemOnStartHandler(String dateTime, String httpServerPortNumber);

  public static native void ClrSystemAllocatedEvaluatorHandlerOnNext(
      long handle,
      AllocatedEvaluatorBridge javaEvaluatorBridge,
      InteropLogger interopLogger
  );

  public static native void ClrSystemActiveContextHandlerOnNext(
      long handle,
      ActiveContextBridge javaActiveContextBridge,
      InteropLogger interopLogger
  );

  public static native void ClrSystemEvaluatorRequstorHandlerOnNext(
      long handle,
      EvaluatorRequestorBridge javaEvluatorRequstorBridge,
      InteropLogger interopLogger
  );

  public static native void ClrSystemTaskMessageHandlerOnNext(
      long handle,
      byte[] mesage,
      TaskMessageBridge javaTaskMessageBridge,
      InteropLogger interopLogger
  );

  public static native void ClrSystemFailedTaskHandlerOnNext(
      long handle,
      FailedTaskBridge failedTaskBridge,
      InteropLogger interopLogger
  );

  public static native void ClrSystemHttpServerHandlerOnNext(
      long handle,
      HttpServerEventBridge httpServerEventBridge,
      InteropLogger interopLogger
  );

  public static native void ClrSystemFailedEvaluatorHandlerOnNext(
      long handle,
      FailedEvaluatorBridge failedEvaluatorBridge,
      InteropLogger interopLogger
  );

  public static native void ClrSystemCompletedTaskHandlerOnNext(
      long handle,
      CompletedTaskBridge completedTaskBridge,
      InteropLogger interopLogger
  );

  public static native void ClrSystemRunningTaskHandlerOnNext(
      long handle,
      RunningTaskBridge runningTaskBridge,
      InteropLogger interopLogger
  );

  public static native void ClrSystemSupendedTaskHandlerOnNext(
      long handle,
      SuspendedTaskBridge suspendedTaskBridge
  );

  public static native void ClrSystemCompletdEvaluatorHandlerOnNext(
      long handle,
      CompletedEvaluatorBridge completedEvaluatorBridge
  );

  public static native void ClrSystemClosedContextHandlerOnNext(
      long handle,
      ClosedContextBridge closedContextBridge
  );

  public static native void ClrSystemFailedContextHandlerOnNext(
      long handle,
      FailedContextBridge failedContextBridge
  );

  public static native void ClrSystemContextMessageHandlerOnNext(
      long handle,
      ContextMessageBridge contextMessageBridge
  );

  public static native void ClrSystemDriverRestartHandlerOnNext(
      long handle
  );

  public static native void ClrSystemDriverRestartActiveContextHandlerOnNext(
      long handle,
      ActiveContextBridge activeContextBridge
  );

  public static native void ClrSystemDriverRestartRunningTaskHandlerOnNext(
      long handle,
      RunningTaskBridge runningTaskBridge
  );
}