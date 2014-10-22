/**
 * Copyright (C) 2014 Microsoft Corporation
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
package com.microsoft.wake;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;

/*
 * Default parameters for Wake
 */
public final class WakeParameters {

  public final static int MAX_FRAME_LENGTH = 1*1024*1024;

  public final static long EXECUTOR_SHUTDOWN_TIMEOUT = 1000;

  public final static long REMOTE_EXECUTOR_SHUTDOWN_TIMEOUT = 10000;

  @NamedParameter(doc="Maximum frame length unit", default_value="" + MAX_FRAME_LENGTH)
  public final static class MaxFrameLength implements Name<Integer> {}

  @NamedParameter(doc="Executor shutdown timeout", default_value="" + EXECUTOR_SHUTDOWN_TIMEOUT)
  public final static class ExecutorShutdownTimeout implements Name<Integer> {}

  @NamedParameter(doc="Remote send timeout", default_value="" + REMOTE_EXECUTOR_SHUTDOWN_TIMEOUT)
  public final static class RemoteSendTimeout implements Name<Integer> {}
}
