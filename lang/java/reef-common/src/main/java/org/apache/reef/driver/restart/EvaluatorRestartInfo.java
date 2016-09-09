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
import org.apache.reef.runtime.common.driver.resourcemanager.ResourceEventImpl;
import org.apache.reef.runtime.common.driver.resourcemanager.ResourceRecoverEvent;

/**
 * An object that encapsulates the information needed to construct an
 * {@link org.apache.reef.runtime.common.driver.evaluator.EvaluatorManager} for a recovered evaluator
 * on restart.
 */
@Private
@DriverSide
@Unstable
public final class EvaluatorRestartInfo {

  private final ResourceRecoverEvent resourceRecoverEvent;

  private EvaluatorRestartState evaluatorRestartState;

  /**
   * Creates an {@link EvaluatorRestartInfo} object that represents the information of an evaluator that is expected
   * to recover.
   */
  public static EvaluatorRestartInfo createExpectedEvaluatorInfo(final ResourceRecoverEvent resourceRecoverEvent) {
    return new EvaluatorRestartInfo(resourceRecoverEvent, EvaluatorRestartState.EXPECTED);
  }

  private EvaluatorRestartInfo(
      final ResourceRecoverEvent resourceRecoverEvent, final EvaluatorRestartState evaluatorRestartState) {

    this.resourceRecoverEvent = resourceRecoverEvent;
    this.evaluatorRestartState = evaluatorRestartState;
  }

  /**
   * Creates an {@link EvaluatorRestartInfo} object that represents the information of an evaluator that
   * has failed on driver restart.
   */
  public static EvaluatorRestartInfo createFailedEvaluatorInfo(final String evaluatorId) {

    final ResourceRecoverEvent resourceRecoverEvent =
        ResourceEventImpl.newRecoveryBuilder().setIdentifier(evaluatorId).build();

    return new EvaluatorRestartInfo(resourceRecoverEvent, EvaluatorRestartState.FAILED);
  }

  /**
   * @return the {@link ResourceRecoverEvent} that contains the information (e.g. resource MB, node ID, Evaluator ID...)
   * needed to reconstruct the {@link org.apache.reef.runtime.common.driver.evaluator.EvaluatorManager} of the
   * recovered evaluator on restart.
   */
  public ResourceRecoverEvent getResourceRecoverEvent() {
    return this.resourceRecoverEvent;
  }

  /**
   * @return the current process of the restart.
   */
  public EvaluatorRestartState getEvaluatorRestartState() {
    return this.evaluatorRestartState;
  }

  /**
   * sets the current process of the restart.
   */
  public boolean setEvaluatorRestartState(final EvaluatorRestartState to) {

    if (this.evaluatorRestartState.isLegalTransition(to)) {
      this.evaluatorRestartState = to;
      return true;
    }

    return false;
  }
}
