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

/**
 * Subscription of a handler.
 * @param <T> type type of the token that is used to identify the subscription.
 */
class SubscriptionHandler<T> implements AutoCloseable {

  interface Unsubscriber<T> {
    void unsubscribe(final T token);
  }

  private final T token;
  private final Unsubscriber<T> unsubscriber;

  /**
   * Constructs a subscription.
   * @param token Token for finding the subscription.
   * @param unsubscriber unsubscribe method of the the container that manages handlers.
   */
  SubscriptionHandler(final T token, final Unsubscriber<T> unsubscriber) {
    this.token = token;
    this.unsubscriber = unsubscriber;
  }

  /**
   * Gets the token of this subscription.
   * @return the token of this subscription.
   */
  public T getToken() {
    return this.token;
  }

  /**
   * Cancels this subscription.
   * @throws Exception if cannot unsubscribe.
   */
  @Override
  public void close() throws Exception {
    this.unsubscriber.unsubscribe(this.token);
  }
}
