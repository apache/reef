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
package com.microsoft.wake.remote.impl;

/**
 * Subscription of a handler
 * 
 * @param <T> type
 */
public class Subscription<T> implements AutoCloseable {

  private final HandlerContainer<T> container;
  private final T token;
  
  /**
   * Constructs a subscription 
   * 
   * @param token the token for finding the subscription
   * @param handlerContainer the container managing handlers
   */
  public Subscription(T token, HandlerContainer<T> handlerContainer) {
    this.token = token;
    this.container = handlerContainer;
  }
  
  /**
   * Gets the token of this subscription
   * 
   * @return the token of this subscription
   */
  public T getToken() {
    return token;
  }
  
  /**
   * Unsubscribes this subscription
   * 
   * @throws Exception
   */
  @Override
  public void close() throws Exception {
    container.unsubscribe(this);
  }

}
