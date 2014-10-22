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


import java.util.Random;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import com.microsoft.wake.metrics.Histogram;
import com.microsoft.wake.metrics.UniformHistogram;

public class MetricsTest {

  @Rule public TestName name = new TestName();

  final String logPrefix = "TEST ";

  @Test
  public void testHistogram() throws Exception {
    System.out.println(logPrefix + name.getMethodName());
    
    Histogram histogram = new UniformHistogram(10, 100);
    Random rand = new Random(1);
    for (int i = 0; i<1000000; ++i) {
      long value = rand.nextLong() % 1000;
      value = value >= 0 ? value : (-1)*value;
      histogram.update(value);
    }
    
    for (int i=0; i<histogram.getNumBins(); ++i) {
      histogram.getValue(i);
    }
  }
}
