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
import org.apache.reef.io.network.group.impl.operators.BroadcastReceiver;
import org.apache.reef.io.network.group.impl.operators.BroadcastSender;
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * MPI Broadcast operator.
 * <p>
 * The sender or root send's an element that is received by all the receivers or other tasks.
 * <p>
 * This is an asymmetric operation and hence the differentiation b/w Sender and Receiver.
 */
public interface Broadcast {

  /**
   * Sender or Root.
   */
  @DefaultImplementation(BroadcastSender.class)
  interface Sender<T> extends GroupCommOperator {

    /**
     * Send element to all receivers.
     */
    void send(T element) throws NetworkException, InterruptedException;
  }

  /**
   * Receivers or Non-roots.
   */
  @DefaultImplementation(BroadcastReceiver.class)
  interface Receiver<T> extends GroupCommOperator {

    /**
     * Receiver the element broadcasted by sender.
     *
     * @return the element broadcasted by sender
     */
    T receive() throws NetworkException, InterruptedException;
  }
}
