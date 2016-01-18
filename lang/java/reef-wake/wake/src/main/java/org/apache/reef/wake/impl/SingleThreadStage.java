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
import org.apache.reef.wake.StageConfiguration.Capacity;
import org.apache.reef.wake.StageConfiguration.StageHandler;
import org.apache.reef.wake.StageConfiguration.StageName;

import javax.inject.Inject;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Single thread stage that runs the event handler.
 *
 * @param <T> type
 */
public final class SingleThreadStage<T> extends AbstractEStage<T> {
  private static final Logger LOG = Logger.getLogger(SingleThreadStage.class.getName());

  private final BlockingQueue<T> queue;
  private final Thread thread;
  private final AtomicBoolean interrupted;

  /**
   * Constructs a single thread stage.
   *
   * @param handler  the event handler to execute
   * @param capacity the queue capacity
   */
  @Inject
  public SingleThreadStage(@Parameter(StageHandler.class) final EventHandler<T> handler,
                           @Parameter(Capacity.class) final int capacity) {
    this(handler.getClass().getName(), handler, capacity);
  }

  /**
   * Constructs a single thread stage.
   *
   * @param name     the stage name
   * @param handler  the event handler to execute
   * @param capacity the queue capacity
   */
  @Inject
  public SingleThreadStage(@Parameter(StageName.class) final String name,
                           @Parameter(StageHandler.class) final EventHandler<T> handler,
                           @Parameter(Capacity.class) final int capacity) {
    super(name);
    queue = new ArrayBlockingQueue<T>(capacity);
    interrupted = new AtomicBoolean(false);
    thread = new Thread(new Producer<T>(name, queue, handler, interrupted));
    thread.setName("SingleThreadStage<" + name + ">");
    thread.start();
    StageManager.instance().register(this);
  }

  /**
   * Puts the value to the queue, which will be processed by the handler later.
   * if the queue is full, IllegalStateException is thrown
   *
   * @param value the value
   * @throws IllegalStateException
   */
  @Override
  public void onNext(final T value) {
    beforeOnNext();
    queue.add(value);
  }

  /**
   * Closes the stage.
   *
   * @throws Exception
   */
  @Override
  public void close() throws Exception {
    if (closed.compareAndSet(false, true)) {
      interrupted.set(true);
      thread.interrupt();
    }
  }


  /**
   * Takes events from the queue and provides them to the handler.
   */
  private class Producer<U> implements Runnable {

    private final String name;
    private final BlockingQueue<U> queue;
    private final EventHandler<U> handler;
    private final AtomicBoolean interrupted;

    Producer(final String name, final BlockingQueue<U> queue, final EventHandler<U> handler,
             final AtomicBoolean interrupted) {
      this.name = name;
      this.queue = queue;
      this.handler = handler;
      this.interrupted = interrupted;
    }

    @Override
    public void run() {
      while (true) {
        try {
          final U value = queue.take();
          handler.onNext(value);
          SingleThreadStage.this.afterOnNext();
        } catch (final InterruptedException e) {
          if (interrupted.get()) {
            LOG.log(Level.FINEST, name + " Closing Producer due to interruption");
            break;
          }
        } catch (final Exception t) {
          LOG.log(Level.SEVERE, name + " Exception from event handler", t);
          throw t;
        }
      }
    }
  }

}

