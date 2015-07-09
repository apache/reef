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
package org.apache.reef.io.network;

import org.apache.reef.exception.evaluator.NetworkException;

import java.util.List;

/**
 * Connection between two end-points named by identifiers.
 *
 * @param <T> type
 */
public interface Connection<T> extends AutoCloseable {

  /**
   * Opens the connection.
   *
   * @throws NetworkException
   */
  void open() throws NetworkException;

  /**
   * Writes a message to the connection.
   *
   * @param message
   */
  void write(T message);

  /**
   * Writes a list of messages to the connection.
   *
   * @param messages
   */
  void write(List<T> messages);

  /**
   * Closes the connection.
   *
   * @throws NetworkException
   */
  @Override
  void close() throws NetworkException;
}
