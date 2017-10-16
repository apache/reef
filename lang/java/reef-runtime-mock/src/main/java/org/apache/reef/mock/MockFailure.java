/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */
package org.apache.reef.mock;

import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.task.RunningTask;

import java.util.Collection;

/**
 * Used to fail running REEF entities i.e., Evaluators, Contexts, Tasks.
 */
public interface MockFailure {

  /**
   * @return current Collection of allocated evaluators.
   */
  Collection<AllocatedEvaluator> getCurrentAllocatedEvaluators();

  /**
   * Fail an allocated evaluator.
   * @param evaluator to be failed
   */
  void fail(final AllocatedEvaluator evaluator);

  /**
   * @return current Collection of active contexts
   */
  Collection<ActiveContext> getCurrentActiveContexts();

  /**
   * Fail an ActiveContext.
   * @param context to be failed
   */
  void fail(final ActiveContext context);

  /**
   * @return current Collection of running tasks
   */
  Collection<RunningTask> getCurrentRunningTasks();

  /**
   * Fail a running task.
   * @param task to be failed
   */
  void fail(final RunningTask task);
}
