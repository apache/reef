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
package com.microsoft.reef.runtime.common.launch;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This is used as the Exception handler for REEF client processes (Driver, Evaluator).
 * <p/>
 * It catches all exceptions and sends them to the controlling process.
 * For Evaluators, that is the Driver. For the Driver, that is the Client.
 * <p/>
 * After sending the exception, this shuts down the JVM, as this JVM is then officially dead.
 */
final class REEFUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {
  private static final Logger LOG = Logger.getLogger(REEFUncaughtExceptionHandler.class.getName());
  private final REEFErrorHandler errorHandler;


  /**
   * @param errorHandler
   */
  @Inject
  REEFUncaughtExceptionHandler(final REEFErrorHandler errorHandler) {
    this.errorHandler = errorHandler;
  }

  @Override
  public final synchronized void uncaughtException(final Thread thread, final Throwable throwable) {
    final String msg = "Thread " + thread.getName() + " threw an uncaught exception.";
    LOG.log(Level.SEVERE, msg, throwable);
    this.errorHandler.onNext(new Exception(msg, throwable));
    try {
      this.wait(100); // TODO: Remove
    } catch (final InterruptedException e) {

    }
    this.errorHandler.close();
    LOG.log(Level.SEVERE, "System.exit(1)");
    System.exit(1);
  }

  @Override
  public String toString() {
    return "REEFUncaughtExceptionHandler{" +
        "errorHandler=" + errorHandler +
        '}';
  }
}
