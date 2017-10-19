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
package org.apache.reef.wake.remote.impl;

import org.apache.reef.wake.remote.RemoteIdentifier;
import org.apache.reef.wake.remote.RemoteMessage;

/**
 * Default remote message implementation.
 *
 * @param <T> type
 */
class DefaultRemoteMessage<T> implements RemoteMessage<T> {

  private final RemoteIdentifier id;
  private final T message;

  /**
   * Constructs a default remote message.
   *
   * @param id      the remote identifier
   * @param message the message
   */
  DefaultRemoteMessage(final RemoteIdentifier id, final T message) {
    this.id = id;
    this.message = message;
  }

  /**
   * Gets the identifier part of this remote message.
   *
   * @return the identifier
   */
  @Override
  public RemoteIdentifier getIdentifier() {
    return id;
  }

  /**
   * Gets the message part of this remote message.
   *
   * @return the message
   */
  @Override
  public T getMessage() {
    return message;
  }

  /**
   * Get a human-readable representation of the object.
   * @return a human-readable string.
   */
  @Override
  public String toString() {
    return String.format("DefaultRemoteMessage: { id: %s type: %s message: %s }",
        this.id, this.message.getClass().getCanonicalName(), this.message);
  }
}
