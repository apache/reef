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
 * MPI Reduce Scatter operator.
 * <p>
 * Each task has a list of elements. Assume that each task reduces
 * each element in the list to form a list of reduced elements at a dummy root.
 * The dummy root then keeps the portion of the list assigned to it and
 * scatters the remaining among the other tasks
 */
public interface ReduceScatter<T> extends GroupCommOperator {

  /**
   * Apply this operation on elements where counts specify the distribution of
   * elements to each task. Ordering is assumed to be default.
   * <p>
   * Here counts is of the same size as the entire group not just children.
   *
   * @return List of values that result from applying reduce function on
   * corresponding elements of each list received as a result of
   * applying scatter.
   */
  List<T> apply(List<T> elements, List<Integer> counts) throws InterruptedException, NetworkException;

  /**
   * Apply this operation on elements where counts specify the distribution of
   * elements to each task. Ordering is specified using order
   * <p>
   * Here counts is of the same size as the entire group not just children
   *
   * @return List of values that result from applying reduce function on
   * corresponding elements of each list received as a result of
   * applying scatter.
   */
  List<T> apply(List<T> elements, List<Integer> counts,
                List<? extends Identifier> order) throws InterruptedException, NetworkException;

  /**
   * get {@link org.apache.reef.io.network.group.api.operators.Reduce.ReduceFunction} configured.
   *
   * @return {@link org.apache.reef.io.network.group.api.operators.Reduce.ReduceFunction}
   */
  Reduce.ReduceFunction<T> getReduceFunction();
}
