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

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * The encapsulating class for alive and failed evaluators on driver restart.
 */
@Private
@DriverSide
@Unstable
public class EvaluatorRestartCollection {
  private final Map<String, EvaluatorRestartInfo> aliveEvaluators;
  private final Set<String> failedEvaluators;

  public EvaluatorRestartCollection(final Map<String, EvaluatorRestartInfo> aliveEvaluators,
                                    final Set<String> failedEvaluators) {
    this.aliveEvaluators = Collections.unmodifiableMap(aliveEvaluators);
    this.failedEvaluators = Collections.unmodifiableSet(failedEvaluators);
  }

  /**
   * @return the unmodifiable map of evaluator IDs to their restart information for alive evaluators on driver restart.
   */
  public Map<String, EvaluatorRestartInfo> getAliveEvaluators() {
    return this.aliveEvaluators;
  }

  /**
   * @return the set of evaluator IDs for failed evaluators on driver restart. The returned set is unmodifiable.
   */
  public Set<String> getFailedEvaluators() {
    return this.failedEvaluators;
  }
}
