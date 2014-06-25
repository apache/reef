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
package com.microsoft.reef.webserver;

import com.microsoft.reef.driver.evaluator.EvaluatorDescriptor;
import com.microsoft.tang.annotations.DefaultImplementation;

import java.util.List;
import java.util.Map;

@DefaultImplementation(AvroEvaluatorInfoSerializer.class)
public interface EvaluatorInfoSerializer {

  /**
   * Build AvroEvaluatorsInfo object from raw data
   */
  AvroEvaluatorsInfo toAvro(
      final List<String> ids, final Map<String, EvaluatorDescriptor> evaluators);

  /**
   * Convert AvroEvaluatorsInfo object to JSON string.
   */
  String toString(final AvroEvaluatorsInfo avroEvaluatorsInfo);
}
