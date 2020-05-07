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
package org.apache.reef.wake.rx;

/**
 * Provides a mechanism for receiving push-based notifications. Implicit
 * contract: zero or more calls of onNext, if a sequence terminates, it is
 * terminated by onError or onCompleted in T
 *
 * @param <T>
 */
public interface Observer<T> {

  /**
   * Provides the observer with new data.
   *
   * @param value
   */
  void onNext(T value);

  /**
   * Notifies the observer that the provider has experienced an error
   * condition.
   *
   * @param error
   */
  void onError(Exception error);

  /**
   * Notifies the observer that the provider has finished sending push-based
   * notifications.
   */
  void onCompleted();
}
