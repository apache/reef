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
package com.microsoft.reef.runtime.common.evaluator.activity;

import com.microsoft.reef.driver.activity.ActivityConfigurationOptions;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Tang;
import com.microsoft.tang.exceptions.InjectionException;

/**
 * Thrown by REEF's runtime code when it catches an exception thrown by user code.
 */
public final class ActivityClientCodeException extends Exception {

  private final String activityID;
  private final String contextID;

  /**
   * @param activityID the id of the failed activity.
   * @param contextID  the ID of the context the failed Activity was executing in.
   * @param message    the error message.
   * @param cause      the exception that caused the Activity to fail.
   */
  public ActivityClientCodeException(final String activityID,
                                     final String contextID,
                                     final String message,
                                     final Throwable cause) {
    super("Failure in activity '" + activityID + "' in context '" + contextID + "': " + message, cause);
    this.activityID = activityID;
    this.contextID = contextID;
  }

  /**
   * @return the ID of the failed Activity.
   */
  public String getActivityID() {
    return this.activityID;
  }

  /**
   * @return the ID of the context the failed Activity was executing in.
   */
  public String getContextID() {
    return this.contextID;
  }

  /**
   * Extracts an activity id from the given configuration.
   *
   * @param config
   * @return the activity id in the given configuration.
   * @throws RuntimeException if the configuration can't be parsed.
   */
  public static String getActivityIdentifier(final Configuration config) {
    try {
      return Tang.Factory.getTang().newInjector(config).getNamedInstance(ActivityConfigurationOptions.Identifier.class);
    } catch (final InjectionException ex) {
      throw new RuntimeException("Unable to determine activity identifier. Giving up.", ex);
    }
  }
}
