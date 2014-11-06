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

package org.apache.reef.util.logging;

import com.google.common.base.Stopwatch;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Log time and duration for a scope
 */
public class ReefLoggingScope implements LoggingScope {
  public static final String TOKEN = ":::";
  public static final String START_PREFIX = "START" + TOKEN;
  public static final String EXIT_PREFIX = "EXIT" + TOKEN;
  public static final String DURATION = " Duration = ";

  private final Stopwatch stopWatch;

  private final Logger logger;

  private final String msg;

  private final Object[] params;

  /**
   * A constructor of ReefLoggingScope. It starts the timer and logs the msg
   *
   * @param logger
   * @param msg
   * @param params
   */
  public ReefLoggingScope(final Logger logger, final String msg, final Object params[]) {
    this.logger = logger;
    this.msg = msg;
    this.params = params;
    stopWatch = new Stopwatch();
    stopWatch.start();

    logger.log(Level.INFO, START_PREFIX + msg, params);
  }

  /**
   * A constructor of ReefLoggingScope.  It starts the timer and and logs the msg.
   *
   * @param logger
   * @param msg
   */
  @Inject
  public ReefLoggingScope(final Logger logger, @Parameter(NamedString.class) final String msg) {
    this.logger = logger;
    this.msg = msg;
    this.params = null;
    stopWatch = new Stopwatch();
    stopWatch.start();

    logger.log(Level.INFO, START_PREFIX + msg);
  }

  @NamedParameter
  class NamedString implements Name<String> {}

  /**
   * The close() will be called when the object is to deleted. It stops the timer and logs the time elapsed.
   */
  @Override
  public void close() {
    stopWatch.stop();
    final StringBuilder sb = new StringBuilder();
    final String s = sb.append(EXIT_PREFIX).append(msg).append(DURATION).append(stopWatch.elapsedMillis()).toString();

    if (params == null) {
      logger.log(Level.INFO, s);
    } else {
      logger.log(Level.INFO, s, params);
    }
  }
}