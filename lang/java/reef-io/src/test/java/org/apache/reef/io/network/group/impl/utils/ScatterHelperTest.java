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
package org.apache.reef.io.network.group.impl.utils;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * Tests for util classes related to Scatter.
 */
public final class ScatterHelperTest {

  /**
   * Test that {@code ScatterHelper.getUniformCounts} functions correctly by giving various sets of inputs.
   */
  @Test
  public void testGetUniformCounts() {
    for (int taskCount = 1; taskCount < 100; taskCount++) {
      final int elementCount = 10000;
      final List<Integer> retVals = ScatterHelper.getUniformCounts(elementCount, taskCount);

      int sum = 0;
      int maxVal = Integer.MIN_VALUE;
      int minVal = Integer.MAX_VALUE;
      int prevVal = Integer.MAX_VALUE;
      for (final int retVal : retVals) {
        sum += retVal;
        maxVal = retVal > maxVal ? retVal : maxVal;
        minVal = retVal < minVal ? retVal : minVal;
        assertTrue(prevVal >= retVal); // monotonic (non-increasing) list check
        prevVal = retVal;
      }
      assertEquals(elementCount, sum); // all elements were considered check
      assertEquals(maxVal - minVal, elementCount % taskCount == 0 ? 0 : 1); // uniform distribution check
    }
  }
}
