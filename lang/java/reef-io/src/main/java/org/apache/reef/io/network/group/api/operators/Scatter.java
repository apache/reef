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
import org.apache.reef.io.network.group.impl.operators.ScatterReceiver;
import org.apache.reef.io.network.group.impl.operators.ScatterSender;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.wake.Identifier;

import java.util.List;

/**
 * MPI Scatter operator
 * <p>
 * Scatter a list of elements to the receivers The receivers will receive a
 * sub-list of elements targeted for them. Supports non-uniform distribution
 * through the specification of counts
 */
public interface Scatter {

  /**
   * Sender or Root.
   */
  @DefaultImplementation(ScatterSender.class)
  interface Sender<T> extends GroupCommOperator {

    /**
     * Distributes evenly across task ids sorted lexicographically.
     */
    void send(List<T> elements) throws NetworkException, InterruptedException;

    /**
     * Distributes as per counts across task ids sorted lexicographically.
     */
    void send(List<T> elements, Integer... counts) throws NetworkException, InterruptedException;

    /**
     * Distributes evenly across task ids sorted using order.
     */
    void send(List<T> elements, List<? extends Identifier> order)
        throws NetworkException, InterruptedException;

    /**
     * Distributes as per counts across task ids sorted using order.
     */
    void send(List<T> elements, List<Integer> counts,
              List<? extends Identifier> order) throws NetworkException, InterruptedException;
  }

  /**
   * Receiver or non-roots.
   */
  @DefaultImplementation(ScatterReceiver.class)
  interface Receiver<T> extends GroupCommOperator {
    /**
     * Receive the sub-list of elements targeted for the current receiver.
     *
     * @return list of elements targeted for the current receiver.
     */
    List<T> receive() throws InterruptedException, NetworkException;
  }
}
