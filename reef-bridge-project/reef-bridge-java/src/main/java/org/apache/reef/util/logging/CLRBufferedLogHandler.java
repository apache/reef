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

import org.apache.reef.javabridge.NativeInterop;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.SimpleFormatter;

/**
 * Logging Handler to intercept java logs and transfer them
 * to the CLR side via the reef-bridge.
 * <p/>
 * Logs are buffered to avoid the cost of reef-bridge function calls.
 * A thread is also scheduled to flush the log buffer at a certain interval,
 * in case the log buffer remains unfilled for an extended period of time.
 */
public class CLRBufferedLogHandler extends Handler {
  private static final int BUFFER_LEN = 10;
  private static final int NUM_THREADS = 1;
  private static final long LOG_SCHEDULE_PERIOD = 15;  // seconds
  private SimpleFormatter formatter;
  private ArrayList<LogRecord> logs;
  private boolean driverInitialized;
  private ScheduledThreadPoolExecutor logScheduler;

  @Inject
  public CLRBufferedLogHandler() {
    super();
    this.formatter = new SimpleFormatter();
    this.logs = new ArrayList<LogRecord>();
    this.driverInitialized = false;
    this.logScheduler = new ScheduledThreadPoolExecutor(NUM_THREADS);
  }

  /**
   * Signals the java-bridge has been initialized and that we can begin logging.
   * Usually called from the StartHandler after the driver is up.
   */
  public void setDriverInitialized() {
    synchronized (this) {
      this.driverInitialized = true;
    }
    startLogScheduler();
  }

  /**
   * Called whenever a log message is received on the java side.
   * <p/>
   * Adds the log record to the log buffer. If the log buffer is full and
   * the driver has already been initialized, flush the buffer of the logs.
   */
  @Override
  public void publish(LogRecord record) {
    if (record == null)
      return;

    if (!isLoggable(record))
      return;

    synchronized (this) {
      this.logs.add(record);
      if (!this.driverInitialized || this.logs.size() < BUFFER_LEN)
        return;
    }

    logAll();
  }

  @Override
  public void flush() {
    logAll();
  }

  /**
   * Flushes the remaining buffered logs and shuts down the log scheduler thread.
   */
  @Override
  public synchronized void close() throws SecurityException {
    if (driverInitialized) {
      this.logAll();
    }
    this.logScheduler.shutdown();
  }

  /**
   * Starts a thread to flush the log buffer on an interval.
   * <p/>
   * This will ensure that logs get flushed periodically, even
   * if the log buffer is not full.
   */
  private void startLogScheduler() {
    this.logScheduler.scheduleAtFixedRate(
        new Runnable() {
          @Override
          public void run() {
            CLRBufferedLogHandler.this.logAll();
          }
        }, 0, LOG_SCHEDULE_PERIOD, TimeUnit.SECONDS);
  }

  /**
   * Flushes the log buffer, logging each buffered log message using
   * the reef-bridge log function.
   */
  private void logAll() {
    synchronized (this) {
      final StringBuilder sb = new StringBuilder();
      Level highestLevel = Level.FINEST;
      for (final LogRecord record : this.logs) {
        sb.append(formatter.format(record));
        sb.append("\n");
        if (record.getLevel().intValue() > highestLevel.intValue()) {
          highestLevel = record.getLevel();
        }
      }
      try {
        final int level = getLevel(highestLevel);
        NativeInterop.ClrBufferedLog(level, sb.toString());
      } catch (Exception e) {
        System.err.println("Failed to perform CLRBufferedLogHandler");
      }

      this.logs.clear();
    }
  }

  /**
   * Returns the integer value of the log record's level to be used
   * by the CLR Bridge log function.
   */
  private int getLevel(Level recordLevel) {
    if (recordLevel.equals(Level.OFF)) {
      return 0;
    } else if (recordLevel.equals(Level.SEVERE)) {
      return 1;
    } else if (recordLevel.equals(Level.WARNING)) {
      return 2;
    } else if (recordLevel.equals(Level.ALL)) {
      return 4;
    } else {
      return 3;
    }
  }
}
