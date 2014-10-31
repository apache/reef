/**
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
package org.apache.reef.exception;

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.io.naming.Identifiable;

import java.util.concurrent.ExecutionException;

/**
 * Exception thrown to the Driver when an Evaluator becomes unusable.
 */
@DriverSide
public class EvaluatorException extends ExecutionException implements Identifiable {

  private static final long serialVersionUID = 1L;
  private final transient String evaluatorId;

  public EvaluatorException(final String evaluatorId) {
    super();
    this.evaluatorId = evaluatorId;
  }

  public EvaluatorException(final String evaluatorId, final String message, final Throwable cause) {
    super(message, cause);
    this.evaluatorId = evaluatorId;
  }

  public EvaluatorException(final String evaluatorId, final String message) {
    super(message);
    this.evaluatorId = evaluatorId;
  }


  public EvaluatorException(final String evaluatorId, final Throwable cause) {
    super(cause);
    this.evaluatorId = evaluatorId;
  }

  /**
   * Access the affected Evaluator.
   *
   * @return the affected Evaluator.
   */
  @Override
  public String getId() {
    return this.evaluatorId;
  }

}
