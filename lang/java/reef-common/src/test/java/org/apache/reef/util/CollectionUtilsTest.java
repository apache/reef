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
package org.apache.reef.util;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for CollectionUtils functions.
 */
public final class CollectionUtilsTest {

  @Test
  public void testNullToEmptyNull() {
    Assert.assertNotNull("Method should never return null", CollectionUtils.nullToEmpty(null));
  }

  @Test
  public void testNullToEmptyNull2() {
    final Integer[] array = new Integer[0];
    Assert.assertArrayEquals("Must return an empty array", array, CollectionUtils.nullToEmpty(null));
  }

  @Test
  public void testNullToEmptyIntegers() {
    final Integer[] array = new Integer[] {1, 2, 3};
    Assert.assertArrayEquals("Must return the same array", array, CollectionUtils.nullToEmpty(array));
    Assert.assertTrue("Must return reference to the same object", array == CollectionUtils.nullToEmpty(array));
  }

  @Test
  public void testNullToEmptyZeroLength() {
    final Integer[] array = new Integer[0];
    Assert.assertArrayEquals("Must return the same array", array, CollectionUtils.nullToEmpty(array));
    Assert.assertTrue("Must return reference to the same object", array == CollectionUtils.nullToEmpty(array));
  }
}
