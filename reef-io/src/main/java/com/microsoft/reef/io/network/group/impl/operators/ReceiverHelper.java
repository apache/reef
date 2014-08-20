/**
 * Copyright (C) 2014 Microsoft Corporation
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
package com.microsoft.reef.io.network.group.impl.operators;

import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type;
import com.microsoft.wake.Identifier;

import java.util.List;

/**
 * An interface of a helper for Receivers of asymmetric operators
 * <p/>
 * Accounts for functionality that should be available on a Receiver
 * --receiving one element from a task
 * --receiving a list of elements from a task
 * --receiving a list of elements from a list of tasks
 * --receiving a list of lists of elements from a task
 *
 * @param <T>
 */
public interface ReceiverHelper<T> {
  /**
   * Blocks till one message is received from the specified task
   * Use when only one element is sent per message
   *
   * @param from
   * @param to
   * @param msgType
   * @return received element
   * @throws InterruptedException
   */
  public T receive(Identifier from, Identifier to,
                   GroupCommMessage.Type msgType) throws InterruptedException;

  /**
   * Blocks till one message is received from the specified task
   * Use when a list of elements are sent in a message
   *
   * @param from
   * @param to
   * @param msgType
   * @return list of received elements contained in the received message
   * @throws InterruptedException
   */
  public List<T> receiveList(Identifier from, Identifier to,
                             GroupCommMessage.Type msgType) throws InterruptedException;

  /**
   * Blocks till one message each is received from the specified tasks
   * The return values will be ordered as per the default ordering
   * Use when one value is to be received from each task belonging to
   * a list of tasks
   *
   * @param from
   * @param to
   * @param msgType
   * @return list of elements containing one element from each task
   * @throws InterruptedException
   */
  public List<T> receive(List<? extends Identifier> from, Identifier to,
                         GroupCommMessage.Type msgType) throws InterruptedException;

  /**
   * This is not used in current implementation but will be useful
   * when considering aggregation trees
   * <p/>
   * Use when list of lists needs to be received
   *
   * @param from
   * @param to
   * @param msgType
   * @return
   * @throws InterruptedException
   */
  public List<List<T>> receiveListOfList(Identifier from, Identifier to, Type msgType)
      throws InterruptedException;
}
