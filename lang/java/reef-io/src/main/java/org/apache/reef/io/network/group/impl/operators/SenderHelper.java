/**
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
package org.apache.reef.io.network.group.impl.operators;

import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.proto.ReefNetworkGroupCommProtos;
import org.apache.reef.wake.Identifier;

import java.util.List;

/**
 * An interface of a helper for Senders of asymmetric operators
 * <p/>
 * Accounts for functionality that should be available on a Sender
 * --sending one element to a task
 * --sending a list of elements to a task
 * --sending a list of elements to a list of tasks
 * --sending a list of lists of elements to a task
 * <p/>
 * Please note that these operations are non-blocking
 *
 * @param <T>
 */
public interface SenderHelper<T> {

  /**
   * Asynchronously send a message to a task
   * Use when one element per message has to be sent
   *
   * @param from
   * @param to
   * @param element
   * @param msgType
   * @throws NetworkException
   */
  void send(Identifier from, Identifier to, T element,
            ReefNetworkGroupCommProtos.GroupCommMessage.Type msgType) throws NetworkException;

  /**
   * Asynchronously send a message to a task
   * Use when a list of elements has to be sent in one message
   *
   * @param from
   * @param to
   * @param elements
   * @param msgType
   * @throws NetworkException
   */
  void send(Identifier from, Identifier to, List<T> elements,
            ReefNetworkGroupCommProtos.GroupCommMessage.Type msgType) throws NetworkException;

  /**
   * Asynchronously send elements to tasks with counts determining
   * how elements are distributed
   *
   * @param from
   * @param to
   * @param elements
   * @param counts
   * @param msgType
   * @throws NetworkException
   */
  void send(Identifier from, List<? extends Identifier> to, List<T> elements,
            List<Integer> counts, ReefNetworkGroupCommProtos.GroupCommMessage.Type msgType)
      throws NetworkException;

  /**
   * This is not used in the basic implementation but will be useful
   * when considering aggregation trees
   * <p/>
   * Asynchronously send a List of list of elements to a task
   * Use when a list of lists is to be sent.
   *
   * @param from
   * @param to
   * @param elements
   * @param msgType
   * @throws NetworkException
   */
  void sendListOfList(Identifier from, Identifier to, List<List<T>> elements,
                      ReefNetworkGroupCommProtos.GroupCommMessage.Type msgType) throws NetworkException;
}
