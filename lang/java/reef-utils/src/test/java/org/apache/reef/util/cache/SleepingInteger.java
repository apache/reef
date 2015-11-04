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
package org.apache.reef.util.cache;

import java.util.concurrent.Callable;

/**
 * A mock computation that simulates computation time by sleeping and returns an integer.
 */
class SleepingInteger implements Callable<Integer> {
  private final int returnValue;
  private final long sleepMillis;

  /**
   * Construct the mock value fetcher.
   *
   * @param returnValue value to return
   * @param sleepMillis amount to sleep
   */
  SleepingInteger(final int returnValue, final long sleepMillis) {
    this.returnValue = returnValue;
    this.sleepMillis = sleepMillis;
  }

  @Override
  public Integer call() throws Exception {
    Thread.sleep(sleepMillis);
    return returnValue;
  }
}
