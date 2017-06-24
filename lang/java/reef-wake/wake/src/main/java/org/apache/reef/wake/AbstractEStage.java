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
package org.apache.reef.wake;

import org.apache.reef.wake.metrics.Meter;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An {@link EStage} that implements metering.
 *
 * @param <T> type
 */
public abstract class AbstractEStage<T> implements EStage<T> {

  protected final AtomicBoolean closed;
  protected final String name;
  private final Meter inMeter;

  /**
   * outputs share a single meter.
   */
  private final Meter outMeter;

  /**
   * Constructs an abstract estage.
   *
   * @param stageName the stage name
   */
  public AbstractEStage(final String stageName) {
    this.closed = new AtomicBoolean(false);
    this.name = stageName;
    this.inMeter = new Meter(stageName + "_in");
    this.outMeter = new Meter(stageName + "_out");
  }

  /**
   * Gets the input meter of this stage.
   *
   * @return the input meter
   */
  public Meter getInMeter() {
    return inMeter;
  }

  /**
   * Gets the output meter of this stage.
   *
   * @return the output meter
   */
  public Meter getOutMeter() {
    return outMeter;
  }

  /**
   * Updates the input meter.
   * <p>
   * Stages that want to meter their
   * input must call this each time an event is input.
   */
  protected void beforeOnNext() {
    inMeter.mark(1);
  }

  /**
   * Updates the output meter.
   * <p>
   * Stages that want to meter their
   * output must call this each time an event is output.
   */
  protected void afterOnNext() {
    outMeter.mark(1);
  }

  /**
   * Check if the stage can still accept messages.
   * @return true if the stage is closed, false otherwise.
   */
  public boolean isClosed() {
    return closed.get();
  }

  /**
   * Get human readable representation of the class (used for logging).
   * @return A string that contains stage name.
   */
  @Override
  public String toString() {
    return String.format("Stage:%s:%s", this.getClass().getCanonicalName(), name);
  }
}
