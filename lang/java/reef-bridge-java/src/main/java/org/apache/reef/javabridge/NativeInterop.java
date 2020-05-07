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

import org.apache.reef.annotations.audience.Interop;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.javabridge.generic.DriverRestartCompletedBridge;

/**
 * Java interfaces of CLR/Java bridge.
 * Implementations of the methods can be found at lang/cs/Org.Apache.REEF.Bridge/JavaClrBridge.cpp.
 */
@Private
@Interop(CppFiles = { "JavaClrBridge.h", "JavaClrBridge.cpp" })
public final class NativeInterop {

  public static final String GLOBAL_LIBRARIES_FILENAME = "userSuppliedGlobalLibraries.txt";

  public static native void loadClrAssembly(String filePath);

  public static native void clrBufferedLog(int level, String message);

  public static native void callClrSystemOnStartHandler();

  public static native void clrSystemAllocatedEvaluatorHandlerOnNext(
      long handle,
      AllocatedEvaluatorBridge javaEvaluatorBridge,
      InteropLogger interopLogger,
      String nameServerInfo,
      String evaluatorId
  );

  public static native void clrSystemActiveContextHandlerOnNext(
      long handle,
      ActiveContextBridge javaActiveContextBridge,
      InteropLogger interopLogger
  );

  public static native void clrSystemTaskMessageHandlerOnNext(
      long handle,
      byte[] message,
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

  public static native void callClrSystemOnRestartHandler(
      DriverRestartedBridge driverRestartedBridge
  );

  public static native void clrSystemDriverRestartActiveContextHandlerOnNext(
      long handle,
      ActiveContextBridge activeContextBridge
  );

  public static native void clrSystemDriverRestartRunningTaskHandlerOnNext(
      long handle,
      RunningTaskBridge runningTaskBridge
  );

  public static native void clrSystemDriverRestartCompletedHandlerOnNext(
      long handle,
      DriverRestartCompletedBridge driverRestartCompletedBridge
  );

  public static native void clrSystemDriverRestartFailedEvaluatorHandlerOnNext(
      long handle,
      FailedEvaluatorBridge failedEvaluatorBridge,
      InteropLogger interopLogger
  );

  public static native float clrSystemProgressProviderGetProgress(long handle);

  public static native void clrSystemSetupBridgeHandlerManager(
      String httpServerPortNumber,
      BridgeHandlerManager bridgeHandlerManager,
      EvaluatorRequestorBridge javaEvaluatorRequestorBridge
  );

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private NativeInterop() {
  }
}
