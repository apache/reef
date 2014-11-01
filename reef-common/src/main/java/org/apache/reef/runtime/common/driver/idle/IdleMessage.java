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
package org.apache.reef.runtime.common.driver.idle;

/**
 * A message to be used to indicate that the driver is potentially idle.
 */
public final class IdleMessage {

  private final String componentName;
  private final String reason;
  private final boolean isIdle;

  /**
   * @param componentName the name of the component that is indicating (not) idle, e.g. "Clock"
   * @param reason        the human-readable reason the component indicates (not) idle, "No more alarms scheduled."
   * @param isIdle        whether or not the component is idle.
   */
  public IdleMessage(final String componentName, final String reason, final boolean isIdle) {
    this.componentName = componentName;
    this.reason = reason;
    this.isIdle = isIdle;
  }

  /**
   * @return the name of the component that indicated (not) idle.
   */
  public String getComponentName() {
    return this.componentName;
  }

  /**
   * @return the reason for (no) idleness
   */
  public String getReason() {
    return this.reason;
  }

  /**
   * @return true if this message indicates idle.
   */
  public boolean isIdle() {
    return this.isIdle;
  }
}
