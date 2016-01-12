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
package org.apache.reef.runtime.common.launch;

import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This is used as the Exception handler for REEF client processes (Driver, Evaluator).
 * <p>
 * It catches all exceptions and sends them to the controlling process.
 * For Evaluators, that is the Driver. For the Driver, that is the Client.
 * <p>
 * After sending the exception, this shuts down the JVM, as this JVM is then officially dead.
 */
public final class REEFUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {
  private static final Logger LOG = Logger.getLogger(REEFUncaughtExceptionHandler.class.getName());
  private final Configuration errorHandlerConfig;

  private REEFErrorHandler errorHandler;

  /**
   * @param errorHandlerConfig
   */
  @Inject
  public REEFUncaughtExceptionHandler(final Configuration errorHandlerConfig) {
    this.errorHandlerConfig = errorHandlerConfig;
    this.errorHandler = null;
  }

  @Override
  public synchronized void uncaughtException(final Thread thread, final Throwable throwable) {
    if (this.errorHandler == null) {
      try {
        this.errorHandler = Tang.Factory.getTang().newInjector(this.errorHandlerConfig)
            .getInstance(REEFErrorHandler.class);
      } catch (InjectionException ie) {
        LOG.log(Level.WARNING, "Unable to inject error handler.");
      }
    }

    final String msg = "Thread " + thread.getName() + " threw an uncaught exception.";

    if (this.errorHandler != null) {
      LOG.log(Level.SEVERE, msg, throwable);
      this.errorHandler.onNext(new Exception(msg, throwable));
      try {
        this.wait(100);
      } catch (final InterruptedException expected) {
        // try-catch block used to wait and give process a chance to setup communication with its parent
      }
      this.errorHandler.close();
    }

    LOG.log(Level.SEVERE, msg + " System.exit(1)");
    System.exit(1);
  }

  @Override
  public String toString() {
    return "REEFUncaughtExceptionHandler{" +
        "errorHandler=" + String.valueOf(this.errorHandler) +
        '}';
  }
}
