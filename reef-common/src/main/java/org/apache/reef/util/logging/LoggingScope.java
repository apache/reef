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

package org.apache.reef.util.logging;

import com.google.common.base.Stopwatch;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Log time and duration for a scope
 */
public class LoggingScope implements AutoCloseable {
  public static final String TOKEN = ":::";
  public static final String START_PREFIX = "START" + TOKEN;
  public static final String EXIT_PREFIX = "EXIT" + TOKEN;
  public static final String DURATION = " Duration = ";

  private final Stopwatch stopWatch;

  private final Logger logger;

  private final String msg;

  private final Object[] params;

  public LoggingScope(final Logger logger, final String msg, final Object params[]) {
    this.logger = logger;
    this.msg = msg;
    this.params = params;
    stopWatch = new Stopwatch();
    stopWatch.start();

    logger.log(Level.INFO, START_PREFIX + msg, params);
  }

  public LoggingScope(final Logger logger, final String msg) {
    this.logger = logger;
    this.msg = msg;
    this.params = null;
    stopWatch = new Stopwatch();
    stopWatch.start();

    logger.log(Level.INFO, START_PREFIX + msg);
  }

  public void close() {
    stopWatch.stop();
    final String s = EXIT_PREFIX + msg + DURATION + stopWatch.elapsedMillis();

    if (params == null) {
      logger.log(Level.INFO, s);
    } else {
      logger.log(Level.INFO, s, params);
    }
  }
}
