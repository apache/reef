/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.reef.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SimpleConditionTest {
  private static final Logger LOG = Logger.getLogger(SimpleConditionTest.class.getName());

  /**
   * Verify proper operation when not timeout occurs.
   * @throws Exception An unexpected exception occurred.
   */
  @Test
  public void testNoTimeout() throws Exception {
    LOG.log(Level.INFO, "Starting...");

    final ExecutorService executor = Executors.newCachedThreadPool();
    final SimpleCondition condition = new SimpleCondition();

    FutureTask<FutureTask<Integer>> doTry = new FutureTask<>(new Callable<FutureTask<Integer>>() {
      @Override
      public FutureTask<Integer> call() throws Exception {
        LOG.log(Level.INFO, "doTry executing...");

        // Spawn a future which can be passed back to the caller.
        FutureTask<Integer> task = new FutureTask<>(new Callable<Integer>() {
          @Override
          public Integer call() throws Exception {
            LOG.log(Level.INFO, "doTry sleeping...");
            Thread.sleep(3000);
            LOG.log(Level.INFO, "doTry signaling the condition...");
            condition.signal();
            LOG.log(Level.INFO, "doTry condition is signaled...");
            return 5;
          }
        });
        executor.submit(task);

        return task;
      }
    });

    FutureTask<Integer> doFinally = new FutureTask<>(new Callable<Integer>() {
      public Integer call() {
        LOG.log(Level.INFO, "doFinally executing...");
        return 5;
      }
    });

    condition.await(doTry, doFinally);
    Thread.sleep(3000);
    Assert.assertEquals("No exceptions", doTry.get().get(), doFinally.get());

    executor.shutdownNow();
  }
}
