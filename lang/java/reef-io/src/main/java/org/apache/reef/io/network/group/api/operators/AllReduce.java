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
package org.apache.reef.io.network.group.api.operators;

import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.wake.Identifier;

import java.util.List;

/**
 * MPI All Reduce Operator. Each task applies this operator on an element of
 * type T. The result will be an element which is result of applying a reduce
 * function on the list of all elements on which this operator has been applied
 */
public interface AllReduce<T> extends GroupCommOperator {

  /**
   * Apply the operation on element.
   *
   * @return result of all-reduce on all elements operation was applied on.
   * Reduce function is applied based on default order.
   */
  T apply(T aElement) throws InterruptedException, NetworkException;

  /**
   * Apply the operation on element.
   *
   * @return result of all-reduce on all elements operation was applied on.
   * Reduce function is applied based on specified order.
   */
  T apply(T element, List<? extends Identifier> order) throws InterruptedException, NetworkException;

  /**
   * Get the {@link org.apache.reef.io.network.group.api.operators.Reduce.ReduceFunction} configured.
   *
   * @return {@link org.apache.reef.io.network.group.api.operators.Reduce.ReduceFunction}
   */
  Reduce.ReduceFunction<T> getReduceFunction();
}
