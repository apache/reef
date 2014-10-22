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
package com.microsoft.reef.driver.parameters;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;

/**
 * Number of threads allocated per evaluator to dispatch events from that Evaluator.
 */
@NamedParameter(
    doc = "Number of threads allocated per evaluator to dispatch events from that Evaluator.",
    default_value = "1")
public final class EvaluatorDispatcherThreads implements Name<Integer> {
  private EvaluatorDispatcherThreads() {
  }
}
