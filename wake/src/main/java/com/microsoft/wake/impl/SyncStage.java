/**
 * Copyright 2013 Microsoft.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.wake.impl;

import javax.inject.Inject;


import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.AbstractEStage;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.StageConfiguration.StageHandler;
import com.microsoft.wake.StageConfiguration.StageName;

/**
 * Stage that synchronously executes an event handler
 * 
 * @param <T> type
 */
public final class SyncStage<T> extends AbstractEStage<T> {

  private final EventHandler<T> handler;

  /**
   * Constructs a synchronous stage
   * 
   * @param handler the event handler
   */
  @Inject
  public SyncStage(final @Parameter(StageHandler.class) EventHandler<T> handler) {
    this(handler.getClass().getName(), handler);
  }
  
  /**
   * Constructs a synchronous stage
   * 
   * @name name the stage name
   * @param handler the event handler
   */
  @Inject
  public SyncStage(final @Parameter(StageName.class) String name, 
      final @Parameter(StageHandler.class) EventHandler<T> handler) {
    super(name);
    this.handler = handler;
    StageManager.instance().register(this);
  }

	
  /**
   * Invokes the handler for the event
   * 
   * @param value an event
   */
  @Override
  public void onNext(T value) {
    beforeOnNext();
	handler.onNext(value);
	afterOnNext();
  }

  /**
   * Closes resources
   * 
   * @throws Exception
   */
  @Override
  public void close() throws Exception {
  }

}
