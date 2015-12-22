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
import org.apache.reef.io.serialization.Codec;

import java.io.Serializable;

/**
 * Typed user function. Implement your functions using this interface.
 * TODO[REEF-504]: Clean up Serializable in Vortex.
 * TODO[REEF-1003]: Use reflection instead of serialization when launching VortexFunction.
 *
 * @param <TInput> input type
 * @param <TOutput> output type
 */
@Unstable
public interface VortexFunction<TInput, TOutput> extends Serializable {
  /**
   * @param input of the function
   * @return output of the function
   * @throws Exception thrown here will bubble up in VortexFuture#get as ExecutionException
   * Exception should be thrown only after all resources are released as they cannot be cleared by Vortex
   * For example if threads are spawned here, shut them down before throwing an exception
   */
  TOutput call(TInput input) throws Exception;

  /**
   * Users must define codec for the input. {@link org.apache.reef.vortex.util.VoidCodec} can be used if the input is
   * empty, and {@link org.apache.reef.io.serialization.SerializableCodec} can be used for ({@link Serializable} input.
   * {@link org.apache.reef.vortex.examples.matmul.MatMulInputCodec} is an example of codec for the custom input.
   * @return Codec used to serialize/deserialize the input.
   */
  Codec<TInput> getInputCodec();

  /**
   * Users must define codec for the output. {@link org.apache.reef.vortex.util.VoidCodec} can be used if the output is
   * empty, and {@link org.apache.reef.io.serialization.SerializableCodec} can be used for ({@link Serializable} output.
   * {@link org.apache.reef.vortex.examples.matmul.MatMulOutputCodec} is an example of codec for the custom output.
   * @return Codec used to serialize/deserialize the output.
   */
  Codec<TOutput> getOutputCodec();
}
