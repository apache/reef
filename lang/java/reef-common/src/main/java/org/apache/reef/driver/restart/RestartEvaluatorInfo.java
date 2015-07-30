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

import java.util.Collections;
import java.util.Set;

/**
 * Used to return information on evaluators that have successfully restarted
 * as well as evaluators that have failed.
 */
public final class RestartEvaluatorInfo {

  private final Set<String> recoveredEvaluatorIds;

  private final Set<String> failedEvaluatorIds;

  public RestartEvaluatorInfo(final Set<String> recoveredEvaluatorIds, final Set<String> failedEvaluatorIds) {
    this.recoveredEvaluatorIds = Collections.unmodifiableSet(recoveredEvaluatorIds);
    this.failedEvaluatorIds = Collections.unmodifiableSet(failedEvaluatorIds);
  }

  /**
   * @return the set of evaluator IDs of evaluators that have successfully survived driver restart.
   */
  public Set<String> getRecoveredEvaluatorIds() {
    return this.recoveredEvaluatorIds;
  }

  /**
   * @return the set of evaluator IDs of evaluators that have failed during driver restart.
   */
  public Set<String> getFailedEvaluatorIds() {
    return this.failedEvaluatorIds;
  }
}
