/**
 * Copyright (C) 2013 Microsoft Corporation
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
package com.microsoft.reef.client;

import com.microsoft.reef.common.AbstractFailure;
import com.microsoft.reef.annotations.Provided;
import com.microsoft.reef.annotations.audience.ClientSide;
import com.microsoft.reef.annotations.audience.Public;

/**
 * An error message that REEF Client receives when there is a user error in REEF job.
 */
@Public
@ClientSide
@Provided
public final class FailedJob extends AbstractFailure {

  /**
   * Create an error message given the entity ID and Java Exception.
   * All accessor methods are provided by the base class.
   *
   * @param id ID of the entity (e.g. the Evaluator) that caused the error. Cannot be null.
   * @param cause Java exception that caused the error. Cannot be null.
   */
  public FailedJob(final String id, final Throwable cause) {
    super(id, cause);
  }
}
