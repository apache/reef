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


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.logging.*;

/**
 * A java.util.logging.Handler that logs to (H)DFS.
 */
public final class DFSHandler extends Handler {

  private static final String CLASS_NAME = DFSHandler.class.getName();
  public static final String DFS_PATH_OPTION = CLASS_NAME + ".folder";
  public static final String FORMATTER_OPTION = CLASS_NAME + ".formatter";
  private final StreamHandler streamHandler;
  private final OutputStream logOutputStream;

  @Inject
  public DFSHandler() throws IOException {
    final LogManager logManager = LogManager.getLogManager();
    final String fileName = logManager.getProperty(DFS_PATH_OPTION) + "/log.txt";
    logOutputStream = FileSystem.get(new Configuration()).create(new Path(fileName), true);
    final Formatter logFormatter = getInstance(logManager.getProperty(FORMATTER_OPTION), new SimpleFormatter());
    this.streamHandler = new StreamHandler(logOutputStream, logFormatter);
  }

  private static <T> T getInstance(final String className, final T defaultValue) {
    try {
      final Class aClass = ClassLoader.getSystemClassLoader().loadClass(className);
      return (T) aClass.newInstance();
    } catch (final Exception e) {
      return defaultValue;
    }

  }

  @Override
  public Formatter getFormatter() {
    return this.streamHandler.getFormatter();
  }

  @Override
  public void setFormatter(final Formatter formatter) throws SecurityException {
    this.streamHandler.setFormatter(formatter);
  }

  @Override
  public String getEncoding() {
    return this.streamHandler.getEncoding();
  }

  @Override
  public void setEncoding(final String s) throws SecurityException, UnsupportedEncodingException {
    this.streamHandler.setEncoding(s);
  }

  @Override
  public Filter getFilter() {
    return this.streamHandler.getFilter();
  }

  @Override
  public void setFilter(final Filter filter) throws SecurityException {
    this.streamHandler.setFilter(filter);
  }

  @Override
  public ErrorManager getErrorManager() {
    return this.streamHandler.getErrorManager();
  }

  @Override
  public void setErrorManager(final ErrorManager errorManager) {
    this.streamHandler.setErrorManager(errorManager);
  }

  @Override
  protected void reportError(final String s, final Exception e, final int i) {
    super.reportError(s, e, i);
  }

  @Override
  public synchronized Level getLevel() {
    return this.streamHandler.getLevel();
  }

  @Override
  public synchronized void setLevel(final Level level) throws SecurityException {
    this.streamHandler.setLevel(level);
  }

  @Override
  public boolean isLoggable(final LogRecord logRecord) {
    return this.streamHandler.isLoggable(logRecord);
  }

  @Override
  public void publish(final LogRecord logRecord) {
    this.streamHandler.publish(logRecord);
  }

  @Override
  public void flush() {
    this.streamHandler.flush();
    try {
      this.logOutputStream.flush();
    } catch (final IOException ignored) {
      // Eating it as it has nowhere to go.
    }
  }

  @Override
  public void close() throws SecurityException {
    this.streamHandler.close();
    try {
      this.logOutputStream.close();
    } catch (final IOException ignored) {
      // Eating it as it has nowhere to go.
    }
  }
}
