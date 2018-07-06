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

package org.apache.reef.bridge.driver.common.grpc;

import io.grpc.stub.StreamObserver;
import org.apache.reef.annotations.audience.Private;

/**
 * Used to assist with finalizing gRPC server calls.
 * @param <T> server RPC return value
 */
@Private
public final class ObserverCleanup<T> implements AutoCloseable {

  private final StreamObserver<T> observer;
  private final T nextValue;

  public static <V> ObserverCleanup<V> of(
      final StreamObserver<V> observer) {
    return of(observer, null);
  }

  public static <V> ObserverCleanup<V> of(
      final StreamObserver<V> observer, final V nextValue) {
    return new ObserverCleanup<>(observer, nextValue);
  }

  private ObserverCleanup(final StreamObserver<T> observer, final T nextValue) {
    this.observer = observer;
    this.nextValue = nextValue;
  }

  @Override
  public void close() {
    this.observer.onNext(this.nextValue);
    this.observer.onCompleted();
  }
}
