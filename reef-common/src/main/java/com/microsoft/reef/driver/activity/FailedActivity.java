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
package com.microsoft.reef.driver.activity;

import com.microsoft.reef.annotations.Provided;
import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.annotations.audience.Public;
import com.microsoft.reef.driver.contexts.ActiveContext;
import com.microsoft.reef.io.naming.Identifiable;
import com.microsoft.reef.util.Optional;

/**
 * Represents a failed Activity.
 */
@DriverSide
@Provided
@Public
public interface FailedActivity extends Identifiable {

  /**
   * Access the context the activity ran (and crashed) on, if it could be recovered.
   * <p/>
   * An ActiveContext is given when the activity fails but the context remains alive. On context failure, the context
   * also fails and is surfaced via the FailedContext event.
   * <p/>
   * Note that receiving an ActiveContext here is no guarantee that the context (and evaluator) are in a consistent
   * state. Application developers need to investigate the reason available via getReason() to make that call.
   *
   * @return the context the Activity ran on.
   */
  public Optional<ActiveContext> getActiveContext();

  /**
   * @return the cause of the failure, if one was caught on the evaluator side.
   */
  public Optional<Throwable> getReason();

  /**
   * @return the Activity Identifier
   */
  @Override
  public String getId();

}
