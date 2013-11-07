/**
 * Copyright (C) 2013 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.io.network.group.operators;

import com.microsoft.reef.exception.evaluator.NetworkException;
import com.microsoft.reef.io.network.group.impl.operators.basic.AllReduceOp;
import com.microsoft.reef.io.network.group.operators.Reduce.ReduceFunction;
import com.microsoft.tang.annotations.DefaultImplementation;
import com.microsoft.wake.Identifier;

import java.util.List;

/**
 * MPI All Reduce Operator. Each activity applies this operator on an element of
 * type T. The result will be an element which is result of applying a reduce
 * function on the list of all elements on which this operator has been applied
 * 
 * @author shravan
 * @param <T>
 */
@DefaultImplementation(AllReduceOp.class)
public interface AllReduce<T> {

  /**
   * Apply the operation on element.
   * 
   * @param aElement
   * @return result of all-reduce on all elements operation was applied on.
   *         Reduce function is applied based on default order.
   * 
   * @throws InterruptedException
   * 
   * @throws NetworkException
   */
  T apply(T aElement) throws InterruptedException, NetworkException;

  /**
   * Apply the operation on element.
   * 
   * @param element
   * @param order
   * @return result of all-reduce on all elements operation was applied on.
   *         Reduce function is applied based on specified order.
   * @throws InterruptedException
   * @throws NetworkException
   */
  T apply(T element, List<? extends Identifier> order)
      throws InterruptedException, NetworkException;

  /**
   * Get the {@link ReduceFunction} configured.
   * 
   * @return {@link ReduceFunction}
   */
  Reduce.ReduceFunction<T> getReduceFunction();
}
