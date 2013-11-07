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
package com.microsoft.reef.driver;

import com.microsoft.reef.annotations.Provided;
import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.annotations.audience.Public;
import com.microsoft.tang.Configuration;

/**
 * Base interface for classes that support the simultaneous submission of both Context and Activity configurations.
 */
@DriverSide
@Provided
@Public
public interface ContextAndActivitySubmittable {
  /**
   * Submit a Context and an Activity.
   * <p/>
   * The semantics of this call are the same as first submitting the context and then, on the fired ActiveContext event
   * to submit the Activity. The performance of this will be better, though as it potentially saves some roundtrips on
   * the network.
   * <p/>
   * REEF will not fire an ActiveContext as a result of this. Instead, it will fire a ActivityRunning event.
   *
   * @param contextConfiguration  the Configuration of the EvaluatorContext. See ContextConfiguration for details.
   * @param activityConfiguration the Configuration of the Activity. See ActivityConfiguration for details.
   */
  public void submitContextAndActivity(final Configuration contextConfiguration, final Configuration activityConfiguration);

  /**
   * Subkit a Context with Services and an Activity.
   * <p/>
   * The semantics of this call are the same as first submitting the context and services and then, on the fired
   * ActiveContext event to submit the Activity. The performance of this will be better, though as it potentially saves
   * some roundtrips on the network.
   * <p/>
   * REEF will not fire an ActiveContext as a result of this. Instead, it will fire a ActivityRunning event.
   *
   * @param contextConfiguration
   * @param serviceConfiguration
   * @param activityConfiguration
   */
  public void submitContextAndServiceAndActivity(final Configuration contextConfiguration,
                                                 final Configuration serviceConfiguration,
                                                 final Configuration activityConfiguration);

}
