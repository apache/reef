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
package org.apache.reef.util.logging;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Formatter;
import java.util.logging.LogManager;
import java.util.logging.LogRecord;

/**
 * A denser logging format for REEF that is similar to the standard SimpleFormatter.
 * <p>
 * The following config properties are available:
 * <p>
 * * `org.apache.reef.util.logging.ThreadLogFormatter.format`
 * is a format string for String.format() that takes same arguments and in
 * the same order as the standard SimpleFormatter, plus the thread name:
 * 1. date
 * 2. class and method name
 * 3. logger name
 * 4. logging level
 * 5. message
 * 6. stack trace
 * 7. thread name
 * <p>
 * * `org.apache.reef.util.logging.ThreadLogFormatter.dropPrefix`
 * contains a comma-separated list of package name prefixes that should be
 * removed from the class name for logging. e.g. value `com.microsoft.,org.apache.`
 * will have the formatter write class `org.apache.reef.util.logging.Config` as
 * `reef.util.logging.Config`. (Note the dot at the end of the prefix).
 */
public final class ThreadLogFormatter extends Formatter {

  private static final String DEFAULT_FORMAT = "%1$tF %1$tT,%1$tL %4$s %2$s %7$s | %5$s%6$s%n";

  private final List<String> dropPrefix = new ArrayList<>();
  private final Date date = new Date();
  private final String logFormat;

  public ThreadLogFormatter() {

    super();
    final LogManager logManager = LogManager.getLogManager();
    final String className = this.getClass().getName();

    final String format = logManager.getProperty(className + ".format");
    this.logFormat = format != null ? format : DEFAULT_FORMAT;

    final String rawDropStr = logManager.getProperty(className + ".dropPrefix");
    if (rawDropStr != null) {
      for (String prefix : rawDropStr.trim().split(",")) {
        prefix = prefix.trim();
        if (!prefix.isEmpty()) {
          this.dropPrefix.add(prefix);
        }
      }
    }
  }

  /**
   * Format the log string. Internally, it uses `String.format()` that takes same
   * arguments and in the same order as the standard SimpleFormatter, plus the thread name:
   * 1. date
   * 2. class and method name
   * 3. logger name
   * 4. logging level
   * 5. message
   * 6. stack trace
   * 7. thread name
   *
   * @return string to be written to the log.
   */
  @Override
  public String format(final LogRecord logRecord) {
    this.date.setTime(System.currentTimeMillis());
    return String.format(
        this.logFormat,
        this.date,
        this.trimPrefix(logRecord.getSourceClassName()) + "." + logRecord.getSourceMethodName(),
        logRecord.getLoggerName(),
        logRecord.getLevel().getLocalizedName(),
        formatMessage(logRecord),
        this.getStackTrace(logRecord.getThrown()),
        Thread.currentThread().getName());
  }

  /**
   * Check if the class name starts with one of the prefixes specified in `dropPrefix`,
   * and remove it. e.g. for class name `org.apache.reef.util.logging.Config` and
   * prefix `com.microsoft.` (note the trailing dot), the result will be
   * `reef.util.logging.Config`
   */
  private String trimPrefix(final String className) {
    for (final String prefix : this.dropPrefix) {
      if (className.startsWith(prefix)) {
        return className.substring(prefix.length());
      }
    }
    return className;
  }

  /**
   * @return a string that contains stack trace of a given exception.
   * if `error` is null, return an empty string.
   */
  private String getStackTrace(final Throwable error) {
    if (error != null) {
      try (StringWriter sw = new StringWriter();
           PrintWriter pw = new PrintWriter(sw)) {
        pw.println();
        error.printStackTrace(pw);
        return sw.toString();
      } catch (final IOException ex) {
        // should never happen
        throw new RuntimeException("Unexpected error while logging stack trace", ex);
      }
    }
    return "";
  }
}
