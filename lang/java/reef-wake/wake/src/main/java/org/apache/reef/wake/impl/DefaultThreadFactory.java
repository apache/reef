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
package org.apache.reef.wake.impl;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A default thread factory implementation that names created threads
 */
public final class DefaultThreadFactory implements ThreadFactory {
  private static final AtomicInteger poolNumber = new AtomicInteger(1);
  private final ThreadGroup group;
  private final AtomicInteger threadNumber = new AtomicInteger(1);
  private final String prefix;
  private Thread.UncaughtExceptionHandler uncaughtExceptionHandler;

  /**
   * Constructs a default thread factory
   *
   * @param prefix the name prefix of the created thread
   */
  public DefaultThreadFactory(String prefix) {
    SecurityManager s = System.getSecurityManager();
    this.group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
    this.prefix = prefix + "-pool-" + poolNumber.getAndIncrement() + "-thread-";
    this.uncaughtExceptionHandler = null;
  }

  /**
   * Constructs a default thread factory
   *
   * @param prefix                   the name prefix of the created thread
   * @param uncaughtExceptionHandler the uncaught exception handler of the created thread
   */
  public DefaultThreadFactory(String prefix, Thread.UncaughtExceptionHandler uncaughtExceptionHandler) {
    SecurityManager s = System.getSecurityManager();
    this.group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
    this.prefix = prefix + "-pool-" + poolNumber.getAndIncrement() + "-thread-";
    this.uncaughtExceptionHandler = uncaughtExceptionHandler;
  }

  /**
   * Sets a uncaught exception handler
   *
   * @param uncaughtExceptionHandler the uncaught exception handler
   */
  public void setUncaughtExceptionHandler(Thread.UncaughtExceptionHandler uncaughtExceptionHandler) {
    this.uncaughtExceptionHandler = uncaughtExceptionHandler;
  }

  /**
   * Creates a new thread
   *
   * @param r the runnable
   */
  @Override
  public Thread newThread(Runnable r) {
    Thread t = new Thread(group, r, prefix + threadNumber.getAndIncrement(), 0);
    if (t.isDaemon())
      t.setDaemon(false);
    if (t.getPriority() != Thread.NORM_PRIORITY)
      t.setPriority(Thread.NORM_PRIORITY);
    if (uncaughtExceptionHandler != null)
      t.setUncaughtExceptionHandler(uncaughtExceptionHandler);
    return t;
  }

}
