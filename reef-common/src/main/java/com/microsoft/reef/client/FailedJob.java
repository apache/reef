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
package com.microsoft.reef.client;

import com.microsoft.reef.annotations.Provided;
import com.microsoft.reef.annotations.audience.ClientSide;
import com.microsoft.reef.annotations.audience.Public;
import com.microsoft.reef.common.AbstractFailure;
import com.microsoft.reef.util.Optional;

/**
 * An error message that REEF Client receives when there is a user error in REEF job.
 */
@Public
@ClientSide
@Provided
public final class FailedJob extends AbstractFailure {
  /**
   * @param id          Identifier of the Job that produced the error.
   * @param message     One-line error message.
   * @param description Long error description.
   * @param cause       Java Exception that caused the error.
   * @param data        byte array that contains serialized version of the error.
   */
  public FailedJob(final String id,
                   final String message,
                   final Optional<String> description,
                   final Optional<Throwable> cause,
                   final Optional<byte[]> data) {
    super(id, message, description, cause, data);
  }
}
