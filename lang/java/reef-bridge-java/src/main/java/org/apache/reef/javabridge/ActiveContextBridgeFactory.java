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

import net.jcip.annotations.ThreadSafe;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.context.ActiveContext;

import javax.inject.Inject;

/**
 * Factory for ActiveContextBridge instances.
 */
@DriverSide
@ThreadSafe
@Private
public final class ActiveContextBridgeFactory {
  /**
   * This is always instantiated via Tang.
   */
  @Inject
  private ActiveContextBridgeFactory() {
  }

  /**
   * Instantiates a new ActiveContextBridge.
   *
   * @param context the context for which to return an ActiveContextBridge instance.
   * @return a new ActiveContextBridge.
   */
  public ActiveContextBridge getActiveContextBridge(final ActiveContext context) {
    return new ActiveContextBridge(context);
  }
}
