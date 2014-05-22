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

import java.util.logging.Formatter;
import java.util.logging.LogRecord;
import java.util.logging.SimpleFormatter;

/**
 * A denser logging format for REEF.
 */
public final class REEFLoggingFormatter extends Formatter {

  private final SimpleFormatter simpleFormatter = new SimpleFormatter();

  @Override
  public String format(LogRecord logRecord) {
    final StringBuilder result = new StringBuilder();
    result.append(logRecord.getSequenceNumber());
    result.append('[');
    result.append(logRecord.getLevel());
    result.append("] ");
    result.append(logRecord.getSourceClassName().replace("com.microsoft.reef", "c.m.r"));
    result.append('.');
    result.append(logRecord.getSourceMethodName());
    result.append(": ");
    result.append(simpleFormatter.formatMessage(logRecord));
    result.append('\n');
    return result.toString();
  }

}
