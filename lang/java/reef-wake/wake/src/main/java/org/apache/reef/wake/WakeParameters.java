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
package org.apache.reef.wake;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

/**
 * Default parameters for Wake.
 */
public final class WakeParameters {

  public static final int MAX_FRAME_LENGTH = 1 * 1024 * 1024;

  public static final long EXECUTOR_SHUTDOWN_TIMEOUT = 1000;

  public static final long REMOTE_EXECUTOR_SHUTDOWN_TIMEOUT = 20000;

  /**
   * Maximum frame length unit.
   */
  @NamedParameter(doc = "Maximum frame length unit.", default_value = "" + MAX_FRAME_LENGTH)
  public static final class MaxFrameLength implements Name<Integer> {
  }

  /**
   * Executor shutdown timeout.
   */
  @NamedParameter(doc = "Executor shutdown timeout.", default_value = "" + EXECUTOR_SHUTDOWN_TIMEOUT)
  public static final class ExecutorShutdownTimeout implements Name<Integer> {
  }

  /**
   * Remote send timeout.
   */
  @NamedParameter(doc = "Remote send timeout.", default_value = "" + REMOTE_EXECUTOR_SHUTDOWN_TIMEOUT)
  public static final class RemoteSendTimeout implements Name<Integer> {
  }

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private WakeParameters() {
  }
}
