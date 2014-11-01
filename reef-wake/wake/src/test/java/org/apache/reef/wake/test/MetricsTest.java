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
package org.apache.reef.wake.test;


import org.apache.reef.wake.metrics.Histogram;
import org.apache.reef.wake.metrics.UniformHistogram;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.util.Random;

public class MetricsTest {

  final String logPrefix = "TEST ";
  @Rule
  public TestName name = new TestName();

  @Test
  public void testHistogram() throws Exception {
    System.out.println(logPrefix + name.getMethodName());

    Histogram histogram = new UniformHistogram(10, 100);
    Random rand = new Random(1);
    for (int i = 0; i < 1000000; ++i) {
      long value = rand.nextLong() % 1000;
      value = value >= 0 ? value : (-1) * value;
      histogram.update(value);
    }

    for (int i = 0; i < histogram.getNumBins(); ++i) {
      histogram.getValue(i);
    }
  }
}
