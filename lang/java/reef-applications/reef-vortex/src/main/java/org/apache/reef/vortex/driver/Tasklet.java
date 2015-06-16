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
package org.apache.reef.vortex.driver;

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.vortex.api.VortexFunction;
import org.apache.reef.vortex.api.VortexFuture;

import java.io.Serializable;

/**
 * Representation of user task in Driver.
 */
@DriverSide
final class Tasklet<TInput extends Serializable, TOutput extends Serializable> implements Serializable {
  private final int taskletId;
  private final VortexFunction<TInput, TOutput> userTask;
  private final TInput input;
  private final VortexFuture<TOutput> vortexFuture;

  public Tasklet(final int taskletId,
                 final VortexFunction<TInput, TOutput> userTask,
                 final TInput input,
                 final VortexFuture<TOutput> vortexFuture) {
    this.taskletId = taskletId;
    this.userTask = userTask;
    this.input = input;
    this.vortexFuture = vortexFuture;
  }

  public int getId() {
    return taskletId;
  }

  public TInput getInput() {
    return input;
  }

  public VortexFunction<TInput, TOutput> getUserFunction() {
    return userTask;
  }

  /**
   * Called by VortexMaster to let the user know that the task completed.
   */
  public void completed(final TOutput result) {
    vortexFuture.completed(result);
  }

  /**
   * Called by VortexMaster to let the user know that the task threw an exception.
   */
  public void threwException(final Exception exception) {
    vortexFuture.threwException(exception);
  }

  /**
   * For tests.
   */
  public boolean isCompleted() {
    return vortexFuture.isDone();
  }

  @Override
  public String toString() {
    return "Tasklet: " + taskletId;
  }
}
