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
package com.microsoft.reef.util.logging;

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
 * A denser logging format for REEF.
 */
public final class ThreadLogFormatter extends Formatter {

  private static final String DEFAULT_FORMAT = "%1$tF %1$tT,%1$tL %4$s %2$s %7$s | %5$s%6$s%n";

  private final List<String> dropPrefix = new ArrayList<>();
  private final String logFormat;

  public ThreadLogFormatter() {

    super();
    final LogManager logManager = LogManager.getLogManager();
    final String className = this.getClass().getName();

    final String format = logManager.getProperty(className + ".format");
    this.logFormat = format != null ?  format : DEFAULT_FORMAT;

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

  @Override
  public String format(final LogRecord logRecord) {
    return String.format(this.logFormat,
        new Date(System.currentTimeMillis()),
        this.trimPrefix(logRecord.getSourceClassName()) + "." + logRecord.getSourceMethodName(),
        logRecord.getLoggerName(),
        logRecord.getLevel().getLocalizedName(),
        formatMessage(logRecord),
        this.getStackTrace(logRecord.getThrown()),
        Thread.currentThread().getName());
  }

  private String trimPrefix(final String className) {
    for (final String prefix : this.dropPrefix) {
      if (className.startsWith(prefix)) {
        return className.substring(prefix.length());
      }
    }
    return className;
  }

  private String getStackTrace(final Throwable error) {
    if (error != null) {
      try (final StringWriter sw = new StringWriter();
           final PrintWriter pw = new PrintWriter(sw)) {
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
