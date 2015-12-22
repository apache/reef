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
package org.apache.reef.vortex.common;

import org.apache.reef.annotations.Unstable;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.vortex.api.VortexFunction;

/**
 * Request to execute a tasklet.
 */
@Unstable
@Private
public final class TaskletExecutionRequest<TInput, TOutput> implements VortexRequest {
  private final int taskletId;
  private final VortexFunction<TInput, TOutput> userFunction;
  private final TInput input;

  /**
   * @return the type of this VortexRequest.
   */
  @Override
  public RequestType getType() {
    return RequestType.ExecuteTasklet;
  }

  /**
   * Request from Vortex Master to Vortex Worker to execute a tasklet.
   */
  public TaskletExecutionRequest(final int taskletId,
                                 final VortexFunction<TInput, TOutput> userFunction,
                                 final TInput input) {
    this.taskletId = taskletId;
    this.userFunction = userFunction;
    this.input = input;
  }

  /**
   * Execute the function using the input.
   * @return Output of the function in a serialized form.
   */
  public byte[] execute() throws Exception {
    final TOutput output = userFunction.call(input);
    final Codec<TOutput> codec = userFunction.getOutputCodec();
    return codec.encode(output);
  }

  /**
   * Get id of the tasklet.
   */
  @Override
  public int getTaskletId() {
    return taskletId;
  }

  /**
   * Get function of the tasklet.
   */
  public VortexFunction getFunction() {
    return userFunction;
  }

  /**
   * Get input of the tasklet.
   */
  public TInput getInput() {
    return input;
  }
}
