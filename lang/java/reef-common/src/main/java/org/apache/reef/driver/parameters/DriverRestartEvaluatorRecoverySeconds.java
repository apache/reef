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
package org.apache.reef.driver.parameters;

import org.apache.reef.annotations.Unstable;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

/**
 * Represents the amount of time in seconds that the driver restart waits for evaluators to report back.
 * Defaults to 3 minutes. If the value is set to Integer.MAX_VALUE, the driver will wait forever until all
 * expected evaluators report back or fail.
 */
@Unstable
@NamedParameter(doc = "The amount of time in seconds that the driver restart waits for" +
    " evaluators to report back. Defaults to 3 minutes. If the value is set to Integer.MAX_VALUE, " +
    "the driver will wait forever until all expected evaluators report back or fail.",
    default_value = DriverRestartEvaluatorRecoverySeconds.DEFAULT)
public final class DriverRestartEvaluatorRecoverySeconds implements Name<Integer> {

  /**
   * The driver waits forever until all expected evaluators report back or fail.
   */
  public static final String INFINITE = Long.toString(Integer.MAX_VALUE);

  /**
   * Default restart wait for the driver is 3 minutes.
   */
  public static final String DEFAULT = "180";

  private DriverRestartEvaluatorRecoverySeconds(){
  }
}
