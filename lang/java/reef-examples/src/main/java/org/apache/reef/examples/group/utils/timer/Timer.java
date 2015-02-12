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
package org.apache.reef.examples.group.utils.timer;

import java.util.logging.Level;
import java.util.logging.Logger;

public class Timer implements AutoCloseable {

  private static final Logger LOG = Logger.getLogger(Timer.class.getName());

  public static final int MINUTES = 60 * 1000;  // ms
  public static final int HOURS = 60 * MINUTES;

  private final Logger log;
  private final Level level;
  private final String description;
  private final long timeStart;

  public Timer(final Logger log, final String description) {
    this(log, Level.INFO, description);
  }

  public Timer(final String description) {
    this(LOG, Level.INFO, description);
  }

  public Timer(final Logger log, final Level level, final String description) {
    this.log = log;
    this.level = level;
    this.description = description;
    this.timeStart = System.currentTimeMillis();
    this.log.log(this.level, "TIMER Start: {0}", this.description);
  }

  @Override
  public void close() {
    final long timeEnd = System.currentTimeMillis();
    this.log.log(this.level, "TIMER End: {0} took {1} sec.",
        new Object[]{this.description, (timeEnd - this.timeStart) / 1000.0});
  }
}
