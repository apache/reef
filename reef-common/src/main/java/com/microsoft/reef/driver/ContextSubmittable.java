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
package com.microsoft.reef.driver;

import com.microsoft.reef.annotations.Provided;
import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.annotations.audience.Public;
import com.microsoft.tang.Configuration;

/**
 * Base interface for classes that support Context submission.
 */
@DriverSide
@Provided
@Public
public interface ContextSubmittable {

  /**
   * Submit a Context.
   *
   * @param contextConfiguration the Configuration of the EvaluatorContext. See ContextConfiguration for details.
   */
  public void submitContext(final Configuration contextConfiguration);

  /**
   * Submit a Context and a Service Configuration.
   *
   * @param contextConfiguration the Configuration of the EvaluatorContext. See ContextConfiguration for details.
   * @param serviceConfiguration the Configuration for the Services. See ServiceConfiguration for details.
   */
  public void submitContextAndService(final Configuration contextConfiguration, final Configuration serviceConfiguration);

}
