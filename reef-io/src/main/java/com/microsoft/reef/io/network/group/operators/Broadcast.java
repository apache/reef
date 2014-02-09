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
import com.microsoft.reef.io.network.group.impl.operators.basic.BroadcastOp;
import com.microsoft.tang.annotations.DefaultImplementation;

/**
 * MPI Broadcast operator.
 * 
 * The sender or root send's an element that is received by all the receivers or
 * other tasks.
 * 
 * This is an asymmetric operation and hence the differentiation b/w Sender and
 * Receiver.
 * 
 * @author shravan
 * 
 */
public interface Broadcast {

  /**
   * Sender or Root.
   * 
   * @param <T>
   */
  @DefaultImplementation(BroadcastOp.Sender.class)
  public static interface Sender<T> {

    /**
     * Send element to all receivers.
     * 
     * @param element
     * @throws NetworkException
     * @throws InterruptedException
     */
    void send(T element) throws NetworkException, InterruptedException;
  }

  /**
   * Receivers or Non-roots
   * 
   * @param <T>
   */
  @DefaultImplementation(BroadcastOp.Receiver.class)
  public static interface Receiver<T> {

    /**
     * Receiver the element broadcasted by sender.
     * 
     * @return the element broadcasted by sender
     * @throws NetworkException
     * @throws InterruptedException
     */
    T receive() throws NetworkException, InterruptedException;
  }
}
