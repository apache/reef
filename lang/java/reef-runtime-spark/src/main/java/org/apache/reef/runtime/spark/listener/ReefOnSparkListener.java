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
package org.apache.reef.runtime.spark.listener;

import org.apache.spark.scheduler.*;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A custom listener to listen for spark events for our reef job.
 */
public class ReefOnSparkListener extends SparkListener {
  private AtomicBoolean appStarted = new AtomicBoolean(false);
  private AtomicInteger executorCounter = new AtomicInteger(0);

  public Boolean isAppStarted() {
    return appStarted.get();
  }

  public Integer getExecutorCounter() {
    return executorCounter.get();
  }

  @Override
  public void onApplicationStart(final SparkListenerApplicationStart applicationStart) {
    appStarted.set(true);
  }

  @Override
  public void onJobStart(final SparkListenerJobStart jobStart) {
    appStarted.set(true);
  }

  @Override
  public void onExecutorAdded(final SparkListenerExecutorAdded executorAdded) {
    executorCounter.incrementAndGet();
  }

  @Override
  public void onExecutorRemoved(final SparkListenerExecutorRemoved executorRemoved) {
    executorCounter.decrementAndGet();
  }

  @Override
  public void onApplicationEnd(final SparkListenerApplicationEnd applicationEnd) {
    executorCounter.set(0);
  }
}
