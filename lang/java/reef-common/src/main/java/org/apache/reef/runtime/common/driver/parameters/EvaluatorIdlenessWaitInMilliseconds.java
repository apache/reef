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
package org.apache.reef.runtime.common.driver.parameters;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

/**
 * The number of Threads in a Driver to verify the completion of Evaluators.
 * Used by {@link org.apache.reef.runtime.common.driver.evaluator.EvaluatorIdlenessThreadPool}.
 */
@NamedParameter(doc = "The number of milliseconds to wait in a loop to verify the completion of Evaluators.",
    default_value = "500")
public final class EvaluatorIdlenessWaitInMilliseconds implements Name<Long> {
  private EvaluatorIdlenessWaitInMilliseconds(){
  }
}
