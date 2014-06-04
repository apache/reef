package com.microsoft.reef.util.logging;

import com.microsoft.reef.javabridge.*;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.*;
import javax.inject.Inject;

/**
 * Logging Handler to intercept java logs and transfer them
 * to the CLR side via the reef-bridge.
 *
 * Logs are buffered to avoid the cost of reef-bridge function calls.
 * A thread is also scheduled to flush the log buffer at a certain interval,
 * in case the log buffer remains unfilled for an extended period of time.
 */
public class CLRBufferedLogHandler extends Handler {
  private SimpleFormatter formatter;
  private ArrayList<LogRecord> logs;
  private boolean driverInitialized;
  private ScheduledThreadPoolExecutor logScheduler;

  private static final int BUFFER_LEN = 10;
  private static final int NUM_THREADS = 1;
  private static final long LOG_SCHEDULE_PERIOD = 15;  // seconds

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
   *
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
  public void close() throws SecurityException {
    logAll();
    this.logScheduler.shutdown();
  }

  /**
   * Starts a thread to flush the log buffer on an interval. 
   *
   * This will ensure that logs get flushed periodically, even
   * if the log buffer is not full.
   */
  private void startLogScheduler() {
    this.logScheduler.scheduleAtFixedRate(
      new Runnable() {  
        @Override public void run() {
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
      for (LogRecord record : this.logs) {
        sb.append(formatter.format(record));
        sb.append("\n");
        if (record.getLevel().intValue() > highestLevel.intValue()) {
          highestLevel = record.getLevel();
        }

      }
      try {
        final int level = getLevel(highestLevel);
        NativeInterop.ClrBufferedLog(level, sb.toString());
      }
      catch (Exception e) {
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
    }
    else if (recordLevel.equals(Level.SEVERE)) {
      return 1;
    }
    else if (recordLevel.equals(Level.WARNING)) {
      return 2;
    }
    else if (recordLevel.equals(Level.ALL)) {
      return 4;
    }
    else {
      return 3;
    }
  }
}
