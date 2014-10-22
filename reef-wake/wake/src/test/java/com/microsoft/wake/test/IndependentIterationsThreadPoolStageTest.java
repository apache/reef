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
package com.microsoft.wake.test;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import com.microsoft.wake.EventHandler;
import org.junit.Test;

import com.microsoft.wake.impl.IndependentIterationsThreadPoolStage;

public class IndependentIterationsThreadPoolStageTest {

  @Test
  public void testOneIteration() {
    final AtomicInteger x = new AtomicInteger(0);
    final int val = 101;
    IndependentIterationsThreadPoolStage<Integer> dut = new IndependentIterationsThreadPoolStage<>(new EventHandler<Integer>() {
      @Override
      public void onNext(Integer value) {
        x.addAndGet(value);
      }
    }, 1, 1);
    List<Integer> ll = new ArrayList<>();
    ll.add(val);
    dut.onNext(ll);
    try {
      dut.close();
    } catch (Exception e) {
      fail(e.toString());
    }
    assertEquals(val, x.get());
  }
  
  @Test
  public void testOneIterationPerThread() throws InterruptedException {
    final AtomicInteger x = new AtomicInteger(0);
    final int num = 100;
    final List<Integer> ll = new ArrayList<>();
    for (int i=0; i<num; i++) {
      ll.add(i);
    }

    IndependentIterationsThreadPoolStage<Integer> dut = new IndependentIterationsThreadPoolStage<>(new EventHandler<Integer>() {
      @Override
      public void onNext(Integer value) {
        Logger.getAnonymousLogger().info("Yow" +value);
        x.addAndGet(value);
      }
    }, num, 1);
    
    dut.onNext(ll);

    try {
      dut.close();
    } catch (Exception e) {
      fail(e.toString());
    }

    assertEquals((num-1)*num/2, x.get());
  }

}
