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
package org.apache.reef.javabridge;

import org.apache.reef.annotations.Unstable;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;

import java.util.Set;

/**
 * The Java-CLR bridge object for {@link org.apache.reef.driver.restart.DriverRestarted} events.
 */
@Private
@DriverSide
@Unstable
public final class DriverRestartedBridge extends NativeBridge {
  // Used by bridge to extract field. Please take this into consideration when changing the name of the field.
  private final String[] expectedEvaluatorIds;

  public DriverRestartedBridge(final Set<String> expectedEvaluatorIds) {
    this.expectedEvaluatorIds = expectedEvaluatorIds.toArray(new String[expectedEvaluatorIds.size()]);
  }

  public String[] getExpectedEvaluatorIds() {
    return expectedEvaluatorIds;
  }

  @Override
  public void close() throws Exception {
  }
}
