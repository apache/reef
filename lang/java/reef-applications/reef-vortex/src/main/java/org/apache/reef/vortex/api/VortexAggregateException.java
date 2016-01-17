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
package org.apache.reef.vortex.api;

import org.apache.reef.annotations.Unstable;
import org.apache.reef.annotations.audience.Private;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Exception thrown when an aggregate function fails.
 * Call {@link Exception#getCause()} to find the cause of failure in aggregation.
 * Call {@link VortexAggregateException#getInputs()} to get the inputs that correlate
 * with the failure.
 */
@Unstable
public final class VortexAggregateException extends Exception {
  private final List<Object> inputList;

  @Private
  public VortexAggregateException(final Throwable cause, final List<?> inputList) {
    super(cause);
    this.inputList = new ArrayList<>(inputList);
  }

  @Private
  public VortexAggregateException(final String message,
                                  final Throwable cause,
                                  final List<?> inputList) {
    super(message, cause);
    this.inputList = new ArrayList<>(inputList);
  }

  /**
   * @return Inputs that correlate with the aggregation failure.
   */
  public List<Object> getInputs() {
    return Collections.unmodifiableList(inputList);
  }
}
