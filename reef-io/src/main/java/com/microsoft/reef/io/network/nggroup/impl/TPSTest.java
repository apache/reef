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
package com.microsoft.reef.io.network.nggroup.impl;

import com.microsoft.wake.EStage;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.impl.ThreadPoolStage;

import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class TPSTest {
  public static void main(final String[] args) throws InterruptedException {
    final AtomicInteger cntr = new AtomicInteger();
    final EStage<Integer> stage = new ThreadPoolStage<>(new EventHandler<Integer>() {

      @Override
      public void onNext(final Integer arg0) {
        cntr.incrementAndGet();
      }
    }, 5);

    final EStage<Integer> feederStage = new ThreadPoolStage<>(new EventHandler<Integer>() {

      @Override
      public void onNext(final Integer arg0) {
        stage.onNext(arg0);
      }
    }, 5);
    for (int i = 0; i < 1000000; i++) {
      feederStage.onNext(i);
    }
    Thread.sleep(5 * 1000);
    System.out.println(cntr);
  }
}
