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
package org.apache.reef.driver;

import org.apache.reef.annotations.Provided;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Public;
import org.apache.reef.tang.Configuration;

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
  void submitContext(Configuration contextConfiguration);

  /**
   * Submit a Context and a Service Configuration.
   *
   * @param contextConfiguration the Configuration of the EvaluatorContext. See ContextConfiguration for details.
   * @param serviceConfiguration the Configuration for the Services. See ServiceConfiguration for details.
   */
  void submitContextAndService(Configuration contextConfiguration, Configuration serviceConfiguration);

}
