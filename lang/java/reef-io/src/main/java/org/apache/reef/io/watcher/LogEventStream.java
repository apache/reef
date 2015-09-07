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
package org.apache.reef.io.watcher;

import org.apache.reef.annotations.Unstable;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Write events to driver.err using Logger.
 */
@Unstable
public final class LogEventStream implements EventStream {

  private static final Logger LOG = Logger.getLogger(LogEventStream.class.getName());

  @Inject
  private LogEventStream() {
  }

  @Override
  public void onEvent(final EventType type, final String jsonEncodedEvent) {
    LOG.log(Level.INFO, "[{0}] {1}", new Object[]{type, jsonEncodedEvent});
  }
}
