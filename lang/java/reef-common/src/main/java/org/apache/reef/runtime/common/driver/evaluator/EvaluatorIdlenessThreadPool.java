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
package org.apache.reef.runtime.common.driver.evaluator;

import org.apache.commons.lang3.Validate;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.runtime.common.driver.parameters.EvaluatorIdlenessThreadPoolSize;
import org.apache.reef.runtime.common.driver.parameters.EvaluatorIdlenessWaitInMilliseconds;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.impl.DefaultThreadFactory;

import javax.inject.Inject;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Runs threads in a thread pool to check the completion of Evaluators on the closing
 * of an {@link EvaluatorManager} in order to trigger Evaluator idleness checks.
 */
@Private
public final class EvaluatorIdlenessThreadPool {
  private static final Logger LOG = Logger.getLogger(EvaluatorIdlenessThreadPool.class.getName());

  private final ExecutorService executor;
  private final long waitInMillis;

  @Inject
  private EvaluatorIdlenessThreadPool(@Parameter(EvaluatorIdlenessThreadPoolSize.class) final int numThreads,
                                      @Parameter(EvaluatorIdlenessWaitInMilliseconds.class) final long waitInMillis) {

    Validate.isTrue(waitInMillis >= 0, "EvaluatorIdlenessWaitInMilliseconds must be configured to be >= 0");
    Validate.isTrue(numThreads > 0, "EvaluatorIdlenessThreadPoolSize must be configured to be > 0");

    this.waitInMillis = waitInMillis;
    this.executor = Executors.newFixedThreadPool(
        numThreads, new DefaultThreadFactory(EvaluatorIdlenessThreadPool.class.getName()));
  }

  /**
   * Runs a check in the ThreadPool for the {@link EvaluatorManager} to wait for it to finish its
   * Event Handling and check its idleness source.
   * @param manager the {@link EvaluatorManager}
   */
  void runCheckAsync(final EvaluatorManager manager) {
    executor.submit(new Runnable() {
      @Override
      public void run() {
        while (!manager.isClosed()) {
          try {
            Thread.sleep(waitInMillis);
          } catch (final InterruptedException e) {
            LOG.log(Level.SEVERE, "Thread interrupted while waiting for Evaluator to finish.");
            throw new RuntimeException(e);
          }
        }

        manager.checkIdlenessSource();
        LOG.log(Level.FINE, "Evaluator " + manager.getId() + " has finished.");
      }
    });
  }
}
