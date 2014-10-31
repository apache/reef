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

import org.apache.reef.wake.AbstractEStage;
import org.apache.reef.wake.EventHandler;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * This stage uses a thread pool to schedule events in parallel.
 * Should be used when input events are already materialized in a List and
 * can be fired in any order.
 *
 * @param numThreads  fixed number of threads available in the pool
 * @param granularity maximum number of events executed serially. The right choice will balance task spawn overhead with parallelism.
 */
public class IndependentIterationsThreadPoolStage<T> extends AbstractEStage<List<T>> {

  final private int granularity;
  private EventHandler<T> handler;
  private ExecutorService executor;

  public IndependentIterationsThreadPoolStage(EventHandler<T> handler, int numThreads, int granularity) {
    super(handler.getClass().getName());
    this.handler = handler;
    this.executor = Executors.newFixedThreadPool(numThreads);
    this.granularity = granularity;
  }

  private Runnable newTask(final List<T> iterations) {
    return new Runnable() {
      @Override
      public void run() {
        for (T e : iterations) {
          handler.onNext(e);
        }
      }
    };
  }

  @Override
  public void onNext(final List<T> iterations) {
    Logger.getAnonymousLogger().info("Execute new task [" + iterations.size());
    final int size = iterations.size();
    for (int i = 0; i < size; i += granularity) {
      int toIndex = i + granularity;
      toIndex = toIndex > size ? size : toIndex;
      executor.execute(newTask(iterations.subList(i, toIndex)));
    }
  }

  @Override
  public void close() throws Exception {
    executor.shutdown();
    executor.awaitTermination(1000, TimeUnit.DAYS);
  }


}
