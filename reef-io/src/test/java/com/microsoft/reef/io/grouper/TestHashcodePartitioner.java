package com.microsoft.reef.io.grouper;
/**
 * Copyright (C) 2013 Microsoft Corporation
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

import com.microsoft.reef.io.grouper.Grouper.Partitioner;
import com.microsoft.reef.io.grouper.impl.HashcodePartitioner;
import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

public class TestHashcodePartitioner extends TestCase {
  @Test
  public void testIsAllowedInteger() {
    Partitioner<String> dut = new HashcodePartitioner<String>(10);
    Assert.assertNotEquals(Partitioner.NONE, dut.partition("abc"));
    Assert.assertNotEquals(Partitioner.ALL, dut.partition("abc"));
    Assert.assertTrue(dut.partition("abc") >= 0);
    Assert.assertTrue(dut.partition("abc") < 10);
  }

  @Test
  public void testFunctional() {
    Partitioner<File> dut = new HashcodePartitioner<>(13);
    File f = new File("abc");
    int i = dut.partition(f);
    int j = dut.partition(f);
    int k = dut.partition(new File("abc"));
    assertEquals(i, j);
    assertEquals(j, k);
  }
}
