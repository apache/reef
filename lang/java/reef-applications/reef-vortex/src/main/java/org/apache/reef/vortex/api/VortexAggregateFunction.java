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
package org.apache.reef.vortex.api;

import org.apache.reef.annotations.Unstable;
import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.annotations.audience.Public;
import org.apache.reef.io.serialization.Codec;

import java.io.Serializable;
import java.util.List;

/**
 * Typed user function for Local Aggregation. Implement your functions using this interface.
 * TODO[REEF-504]: Clean up Serializable in Vortex.
 * TODO[REEF-1003]: Use reflection instead of serialization when launching VortexFunction.
 *
 * @param <TOutput> output type of the aggregation function and the functions to-be-aggregated.
 */
@Public
@ClientSide
@Unstable
public interface VortexAggregateFunction<TOutput> extends Serializable {

  /**
   * Runs a custom local aggregation function on Tasklets assigned to a VortexWorker.
   * @param taskletOutputs the list of outputs from Tasklets on a Worker.
   * @return the aggregated output of Tasklets.
   * @throws Exception
   */
  TOutput call(final List<TOutput> taskletOutputs) throws VortexAggregateException;

  /**
   * Users must define codec for the AggregationOutput.
   * {@link org.apache.reef.vortex.util.VoidCodec} can be used if the aggregation output is
   * empty, and {@link org.apache.reef.io.serialization.SerializableCodec} can be used for ({@link Serializable}
   * aggregation output.
   * Custom aggregation output Codec can also be supplied.
   * @return Codec used to serialize/deserialize the output.
   */
  Codec<TOutput> getOutputCodec();
}
