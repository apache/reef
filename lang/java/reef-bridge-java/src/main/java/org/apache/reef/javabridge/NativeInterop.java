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

  public static native void loadClrAssembly(final String filePath);

  public static native void clrBufferedLog(final int level, final String message);

  public static native void callClrSystemOnStartHandler();

  public static native void clrSystemAllocatedEvaluatorHandlerOnNext(
      final long handle,
      final AllocatedEvaluatorBridge javaEvaluatorBridge,
      final InteropLogger interopLogger,
      final String nameServerInfo,
      final String evaluatorId
  );

  public static native void clrSystemActiveContextHandlerOnNext(
      final long handle,
      final ActiveContextBridge javaActiveContextBridge,
      final InteropLogger interopLogger
  );

  public static native void clrSystemTaskMessageHandlerOnNext(
      final long handle,
      final byte[] message,
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

  public static native void callClrSystemOnRestartHandler(
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
      final long handle,
      final DriverRestartCompletedBridge driverRestartCompletedBridge
  );

  public static native void clrSystemDriverRestartFailedEvaluatorHandlerOnNext(
      final long handle,
      final FailedEvaluatorBridge failedEvaluatorBridge,
      final InteropLogger interopLogger
  );

  public static native float clrSystemProgressProviderGetProgress(final long handle);

  public static native void clrSystemSetupBridgeHandlerManager(
      final String httpServerPortNumber,
      final BridgeHandlerManager bridgeHandlerManager,
      final EvaluatorRequestorBridge javaEvaluatorRequestorBridge
  );

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private NativeInterop() {
  }
}
