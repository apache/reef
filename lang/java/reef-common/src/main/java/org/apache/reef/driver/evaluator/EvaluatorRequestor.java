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
package org.apache.reef.driver.evaluator;

import org.apache.reef.annotations.Provided;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Public;

/**
 * Interface through which Evaluators can be requested.
 */
@Public
@DriverSide
@Provided
public interface EvaluatorRequestor {

  /**
   * Submit the request for new evaluator.
   * The response will surface in the AllocatedEvaluator message handler.
   */
  void submit(final EvaluatorRequest req);

  /**
   * Remove the submitted EvaluatorRequest.
   * @param requestId to be removed.
   */
  void remove(final String requestId);

  /**
   * Get a new Builder for the evaluator with fluid interface.
   * @return Builder for the evaluator
   */
  EvaluatorRequest.Builder newRequest();
}
