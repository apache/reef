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
import org.apache.reef.util.BuilderUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Represents holds the set of Evaluator information needed to recover EvaluatorManagers
 * on the restarted Driver.
 */
@Private
@DriverSide
@Unstable
public final class RestartEvaluators {
  private final Map<String, EvaluatorRestartInfo> restartEvaluatorsMap;

  private RestartEvaluators(final Map<String, EvaluatorRestartInfo> restartEvaluatorsMap){
    this.restartEvaluatorsMap = BuilderUtils.notNull(restartEvaluatorsMap);
  }

  /**
   * @return true if Evaluator with evaluatorId can be an Evaluator from
   * previous application attempts.
   */
  boolean contains(final String evaluatorId) {
    return restartEvaluatorsMap.containsKey(evaluatorId);
  }

  /**
   * @return The {@link EvaluatorRestartInfo} of an Evaluator from
   * previous application attempts.
   */
  EvaluatorRestartInfo get(final String evaluatorId) {
    return restartEvaluatorsMap.get(evaluatorId);
  }

  /**
   * @return The set of Evaluator IDs of Evaluators from previous
   * application attempts.
   */
  Set<String> getEvaluatorIds() {
    return restartEvaluatorsMap.keySet();
  }

  /**
   * @return a new Builder to build an instance of {@link RestartEvaluators}.
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder to build {@link RestartEvaluators}.
   */
  public static final class Builder implements org.apache.reef.util.Builder<RestartEvaluators>{
    private final Map<String, EvaluatorRestartInfo> restartInfoMap = new HashMap<>();

    private Builder(){
    }

    public boolean addRestartEvaluator(final EvaluatorRestartInfo evaluatorRestartInfo) {
      if (evaluatorRestartInfo == null) {
        return false;
      }

      final String evaluatorId = evaluatorRestartInfo.getResourceRecoverEvent().getIdentifier();
      if (evaluatorId == null || restartInfoMap.containsKey(evaluatorId)) {
        return false;
      }

      restartInfoMap.put(evaluatorId, evaluatorRestartInfo);
      return true;
    }

    @Override
    public RestartEvaluators build() {
      return new RestartEvaluators(Collections.unmodifiableMap(restartInfoMap));
    }
  }
}
