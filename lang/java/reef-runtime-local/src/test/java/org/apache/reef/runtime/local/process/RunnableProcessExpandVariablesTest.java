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
package org.apache.reef.runtime.local.process;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Unit tests for the RunnableProcess utility methods.
 */
public final class RunnableProcessExpandVariablesTest {

  @Test
  public void testExpandEnvironmentVariablesEmpty() {
    final String res = RunnableProcess.expandEnvironmentVariables("");
    Assert.assertEquals("", res);
  }

  @Test
  public void testExpandEnvironmentVariablesNoVars() {
    final String res = RunnableProcess.expandEnvironmentVariables("foo*bar");
    Assert.assertEquals("foo*bar", res);
  }

  @Test
  public void testExpandEnvironmentVariablesPartial1() {
    final String res = RunnableProcess.expandEnvironmentVariables("foo{{bar");
    Assert.assertEquals("foo{{bar", res);
  }

  @Test
  public void testExpandEnvironmentVariablesPartial2() {
    final String res = RunnableProcess.expandEnvironmentVariables("foo{{}}bar");
    Assert.assertEquals("foo{{}}bar", res);
  }

  @Test
  public void testExpandEnvironmentVariablesNonMatching() {
    final String res = RunnableProcess.expandEnvironmentVariables("foo*{{SOME_RANDOM_VAR}}*bar");
    Assert.assertEquals("foo**bar", res);
  }

  @Test
  public void testExpandEnvironmentVariablesMatching() {
    final String path = System.getenv("PATH");
    final String res = RunnableProcess.expandEnvironmentVariables("foo*{{PATH}}*bar");
    Assert.assertEquals("foo*" + path + "*bar", res);
  }

  @Test
  public void testExpandEnvironmentVariablesMatchingMany() {
    final String path = System.getenv("PATH");
    final String res = RunnableProcess.expandEnvironmentVariables("foo*{{PATH}}{{PATH}}*bar");
    Assert.assertEquals("foo*" + path + path + "*bar", res);
  }

  @Test
  public void testExpandEnvironmentVariablesList() {
    final String path = System.getenv("PATH");
    final List<String> res = RunnableProcess.expandEnvironmentVariables(
        Arrays.asList("foo*{{PATH}}", "{{PATH}}*bar", "", "etc"));
    Assert.assertArrayEquals(new String[] {"foo*" + path, path + "*bar", "", "etc"}, res.toArray());
  }
}
