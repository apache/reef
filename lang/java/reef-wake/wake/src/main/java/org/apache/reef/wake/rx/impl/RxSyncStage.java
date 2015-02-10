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
package org.apache.reef.wake.rx.impl;

import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.StageConfiguration.StageName;
import org.apache.reef.wake.StageConfiguration.StageObserver;
import org.apache.reef.wake.impl.StageManager;
import org.apache.reef.wake.rx.AbstractRxStage;
import org.apache.reef.wake.rx.Observer;

import javax.inject.Inject;

/**
 * Stage that executes the observer synchronously
 *
 * @param <T> type
 */
public final class RxSyncStage<T> extends AbstractRxStage<T> {

  private final Observer<T> observer;

  /**
   * Constructs a Rx synchronous stage
   *
   * @param observer the observer
   */
  @Inject
  public RxSyncStage(@Parameter(StageObserver.class) final Observer<T> observer) {
    this(observer.getClass().getName(), observer);
  }

  /**
   * Constructs a Rx synchronous stage
   *
   * @param name     the stage name
   * @param observer the observer
   */
  @Inject
  public RxSyncStage(@Parameter(StageName.class) String name,
                     @Parameter(StageObserver.class) Observer<T> observer) {
    super(name);
    this.observer = observer;
    StageManager.instance().register(this);
  }

  /**
   * Provides the observer with the new value
   *
   * @param value the new value
   */
  @Override
  public void onNext(T value) {
    beforeOnNext();
    observer.onNext(value);
    afterOnNext();
  }

  /**
   * Notifies the observer that the provider has experienced an error
   * condition.
   *
   * @param error the error
   */
  @Override
  public void onError(Exception error) {
    observer.onError(error);
  }

  /**
   * Notifies the observer that the provider has finished sending push-based
   * notifications.
   */
  @Override
  public void onCompleted() {
    observer.onCompleted();
  }

  /**
   * Closes the stage
   *
   * @throws Exception
   */
  @Override
  public void close() throws Exception {
  }

}
