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
