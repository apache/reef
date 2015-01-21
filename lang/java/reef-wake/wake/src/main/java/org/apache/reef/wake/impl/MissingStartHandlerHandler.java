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
package org.apache.reef.wake.impl;

import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The EventHandler used as the default for the Clock.StartHandler event.
 */
public class MissingStartHandlerHandler implements EventHandler<StartTime> {

  @Inject
  private MissingStartHandlerHandler() {
  }

  @Override
  public void onNext(final StartTime value) {
    Logger.getLogger(MissingStartHandlerHandler.class.toString())
        .log(Level.WARNING,
            "No binding to Clock.StartHandler. It is likely that the clock will immediately go idle and close.");
  }
}
