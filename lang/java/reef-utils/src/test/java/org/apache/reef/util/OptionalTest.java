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

import java.util.ArrayList;

/**
 * Tests for Optional.
 */
public class OptionalTest {

  @Test
  public void testEmpty() {
    Assert.assertFalse("An empty Optional should return false to isPresent()",
        Optional.empty().isPresent());
  }

  @Test
  public void testOf() {
    Assert.assertTrue("Optional.of() needs to return an Optional where isPresent() returns true",
        Optional.of(2).isPresent());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testOfNull() {
    final Optional<Integer> o = Optional.of(null);
  }

  @Test
  public void testOfRandom() {
    final double value = Math.random();
    final Optional<Double> o = Optional.of(value);
    Assert.assertEquals(value, (double) o.get(), 1e-12);
  }

  @Test
  public void testOfNullable() {
    Assert.assertFalse(Optional.ofNullable(null).isPresent());
    Assert.assertTrue(Optional.ofNullable(1).isPresent());
    Assert.assertEquals(Optional.ofNullable(1).get(), Integer.valueOf(1));
  }

  @Test
  public void testOrElse() {
    Assert.assertEquals(Optional.empty().orElse(2), 2);
    Assert.assertEquals(Optional.of(1).orElse(2), Integer.valueOf(1));
  }

  @Test
  public void testEquals() {
    Assert.assertEquals(Optional.empty(), Optional.empty());
    Assert.assertEquals(Optional.empty(), Optional.ofNullable(null));
    Assert.assertEquals(Optional.of(1), Optional.of(1));
    Assert.assertEquals(Optional.of("one"), Optional.of("one"));
    Assert.assertFalse(Optional.of("one").equals(Optional.of("two")));
  }

  @Test
  public void testEqualsCornerCases() {

    // We lose type coercion:
    Assert.assertFalse(Optional.of(1L).equals(Optional.of(1)));
    Assert.assertTrue(1L == 1);
    Assert.assertTrue(new Integer(1) == 1L);

    // .equals() isn't typesafe, so we lose compile-time type checking:
    Assert.assertFalse(Optional.of(1L).equals(1));

    Assert.assertFalse(Optional.empty().equals(null));
    Assert.assertFalse(Optional.of(3).equals(3));
    Assert.assertFalse(Optional.of("one").equals(1));

    // Assert.assertFalse("one" == 1); // incompatible operands; does not compile.

    Assert.assertFalse(Optional.of(new ArrayList<>()).equals(Optional.of(new Object[]{})));

    // Incompatible operands; does not compile, though == between objects is almost always a typo:
    // Assert.assertFalse(new java.util.ArrayList() == new java.awt.List());
  }
}
