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
package com.microsoft.wake.rx;

import java.util.concurrent.atomic.AtomicBoolean;

import com.microsoft.wake.metrics.Meter;

/**
 * An {@link RxStage} that implements metering 
 *
 * @param <T> type
 */
public abstract class AbstractRxStage<T> implements RxStage<T> {

  protected final AtomicBoolean closed;
  protected final String name;
  protected final Meter inMeter;
  protected final Meter outMeter;
  
  /**
   * Constructs an abstact rxstage
   * 
   * @param stageName the stage name
   */
  public AbstractRxStage(String stageName) {
    this.closed = new AtomicBoolean(false);
    this.name = stageName;
    this.inMeter = new Meter(stageName+"_in");
    this.outMeter = new Meter(stageName+"_out");
  }
  
  /**
   * Updates the input meter.
   * 
   * Stages that want to meter their
   * input must call this each time an event is input.
   */
  protected void beforeOnNext() {
    inMeter.mark(1);
  }
  
  /**
   * Updates the output meter.
   * 
   * Stages that want to meter their
   * output must call this each time an event is output.
   */
  protected void afterOnNext() {
    outMeter.mark(1);
  }

  /**
   * Gets the input meter of this stage
   * 
   * @return the input meter
   */
  public Meter getInMeter() {
    return inMeter;
  }
  
  /**
   * Gets the output meter of this stage
   * 
   * @return the output meter
   */
  public Meter getOutMeter() {
    return outMeter;
  }
}
