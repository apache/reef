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
package org.apache.reef.runtime.common.launch.parameters;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

/**
 * The launch identifier.
 * An alias has been defined for this named parameter in .Net REEF as
 * Org.Apache.REEF.Common.Runtime.Evaluator.Utils.LaunchID
 */
@NamedParameter(doc = "The launch identifier.", short_name = LaunchID.SHORT_NAME)
public final class LaunchID implements Name<String> {
  public static final String SHORT_NAME = "launch_id";

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private LaunchID() {
  }
}
