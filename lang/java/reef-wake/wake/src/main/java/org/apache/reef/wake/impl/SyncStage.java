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
package org.apache.reef.wake.impl;

import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.AbstractEStage;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.StageConfiguration.ErrorHandler;
import org.apache.reef.wake.StageConfiguration.StageHandler;
import org.apache.reef.wake.StageConfiguration.StageName;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Stage that synchronously executes an event handler.
 *
 * @param <T> type
 */
public final class SyncStage<T> extends AbstractEStage<T> {

  private static final Logger LOG = Logger.getLogger(SyncStage.class.getName());

  private final EventHandler<T> handler;
  private final EventHandler<Throwable> errorHandler;

  /**
   * Constructs a synchronous stage.
   *
   * @param handler the event handler
   */
  @Inject
  public SyncStage(@Parameter(StageHandler.class) final EventHandler<T> handler) {
    this(handler.getClass().getName(), handler, null);
  }

  /**
   * Constructs a synchronous stage.
   *
   * @param name the stage name
   * @param handler the event handler
   */
  @Inject
  public SyncStage(@Parameter(StageName.class) final String name,
                   @Parameter(StageHandler.class) final EventHandler<T> handler) {
    this(name, handler, null);
  }

  /**
   * Constructs a synchronous stage.
   *
   * @param name the stage name
   * @param handler      the event handler
   * @param errorHandler the error handler
   */
  @Inject
  public SyncStage(@Parameter(StageName.class) final String name,
                   @Parameter(StageHandler.class) final EventHandler<T> handler,
                   @Parameter(ErrorHandler.class) final EventHandler<Throwable> errorHandler) {
    super(name);
    this.handler = handler;
    this.errorHandler = errorHandler;
    StageManager.instance().register(this);
  }

  /**
   * Invokes the handler for the event.
   *
   * @param value the event
   */
  @Override
  @SuppressWarnings("checkstyle:illegalcatch")
  public void onNext(final T value) {
    beforeOnNext();
    try {
      handler.onNext(value);
    } catch (final Throwable t) {
      if (errorHandler != null) {
        errorHandler.onNext(t);
      } else {
        LOG.log(Level.SEVERE, name + " Exception from event handler", t);
        throw t;
      }
    }
    afterOnNext();
  }

  /**
   * Closes resources.
   *
   * @throws Exception
   */
  @Override
  public void close() throws Exception {
  }

}
