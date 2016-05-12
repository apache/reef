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
package org.apache.reef.driver.restart;

import org.apache.reef.annotations.Unstable;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.wake.time.event.StartTime;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @see DriverRestarted
 */
@DriverSide
@Private
@Unstable
public final class DriverRestartedImpl implements DriverRestarted {
  private final int resubmissionAttempts;
  private final StartTime startTime;
  private final Set<String> expectedEvaluatorIds;

  DriverRestartedImpl(final int resubmissionAttempts,
                      final StartTime startTime,
                      final RestartEvaluators restartEvaluators) {
    this.resubmissionAttempts = resubmissionAttempts;
    this.startTime = startTime;
    final Set<String> expected = new HashSet<>();

    for (final String evaluatorId : restartEvaluators.getEvaluatorIds()) {
      if (restartEvaluators.get(evaluatorId).getEvaluatorRestartState() == EvaluatorRestartState.EXPECTED) {
        expected.add(evaluatorId);
      }
    }

    this.expectedEvaluatorIds = Collections.unmodifiableSet(expected);
  }

  @Override
  public int getResubmissionAttempts() {
    return resubmissionAttempts;
  }

  @Override
  public StartTime getStartTime() {
    return startTime;
  }

  @Override
  public Set<String> getExpectedEvaluatorIds() {
    return expectedEvaluatorIds;
  }
}
