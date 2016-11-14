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
 * Test for SingletonAsserter.
 */
public final class SingletonAsserterTest {

  private static final class GlobalA { }
  private static final class GlobalB { }

  @Test
  public void testSingletonAsserterGlobal() {
    Assert.assertTrue(SingletonAsserter.assertSingleton(GlobalA.class));
    Assert.assertTrue(SingletonAsserter.assertSingleton(GlobalB.class));
    Assert.assertFalse(SingletonAsserter.assertSingleton(GlobalA.class));
  }

  private static final class LocalA { }
  private static final class LocalB { }

  @Test
  public void testSingletonAsserterScoped() {
    Assert.assertTrue(SingletonAsserter.assertSingleton("A", LocalA.class));
    Assert.assertTrue(SingletonAsserter.assertSingleton("A", LocalB.class));
    Assert.assertTrue(SingletonAsserter.assertSingleton("B", LocalA.class));
    Assert.assertFalse(SingletonAsserter.assertSingleton("A", LocalA.class));
  }

  private static final class Mixed1 { }

  @Test
  public void testSingletonAsserterMixed1() {
    Assert.assertTrue(SingletonAsserter.assertSingleton("A", Mixed1.class));
    Assert.assertTrue(SingletonAsserter.assertSingleton("B", Mixed1.class));
    Assert.assertFalse(SingletonAsserter.assertSingleton(Mixed1.class));
  }

  private static final class Mixed2 { }

  @Test
  public void testSingletonAsserterMixed2() {
    Assert.assertTrue(SingletonAsserter.assertSingleton(Mixed2.class));
    Assert.assertFalse(SingletonAsserter.assertSingleton("A", Mixed2.class));
  }
}
