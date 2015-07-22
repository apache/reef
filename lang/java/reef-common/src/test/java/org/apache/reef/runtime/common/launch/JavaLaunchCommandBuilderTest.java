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
package org.apache.reef.runtime.common.launch;

import org.junit.Test;

import java.util.List;

import static org.apache.reef.runtime.common.launch.JavaLaunchCommandBuilder.JVMOption;
import static org.junit.Assert.*;

public final class JavaLaunchCommandBuilderTest {

  @Test
  public void testParseEqualsOption() {
    final JVMOption equals = JVMOption.parseJVMOption("-XX:PermSize=128m");
    assertEquals("-XX:PermSize", equals.option);
    assertEquals("128m", equals.value);
    assertEquals("=", equals.separator);
    assertEquals("-XX:PermSize=128m", equals.toString());
  }

  @Test
  public void testParseMemoryOption() {
    final JVMOption xmx = JVMOption.parseJVMOption("-Xmx500m");
    assertEquals("-Xmx", xmx.option);
    assertEquals("500m", xmx.value);
    assertEquals("", xmx.separator);
    assertEquals("-Xmx500m", xmx.toString());
  }

  @Test
  public void testParseUnknownOption() {
    final JVMOption unknown = JVMOption.parseJVMOption("-XX:+HeapDumpOnOutOfMemory");
    assertEquals("-XX:+HeapDumpOnOutOfMemory", unknown.option);
    assertEquals("", unknown.value);
    assertEquals("", unknown.separator);
    assertEquals("-XX:+HeapDumpOnOutOfMemory", unknown.toString());
  }

  @Test
  public void testSetMemory() {
    final List<String> command = newBuilder()
        .setMemory(300)
        .build();
    assertMemoryOption(command, "300m");
  }

  @Test
  public void testOverrideMemory() {
    final List<String> command = newBuilder()
        .setMemory(300)
        .setMemory(500)
        .build();
    assertMemoryOption(command, "500m");
  }

  @Test
  public void testSetDefaultMemory() {
    final List<String> command = newBuilder()
        .setDefaultMemory(500)
        .build();
    assertMemoryOption(command, "500m");
  }

  @Test
  public void testOverrideDefaultMemory() {
    final List<String> command = newBuilder()
        .setDefaultMemory(500)
        .setMemory(300)
        .build();
    assertMemoryOption(command, "300m");
  }

  @Test
  public void testAddMemoryOption() {
    final List<String> command = newBuilder()
        .addOption("-Xmx900m")
        .build();
    assertMemoryOption(command, "900m");
  }

  @Test
  public void testOverrideMemoryOption() {
    final List<String> command = newBuilder()
        .setMemory(300)
        .addOption("-Xmx900m")
        .build();
    assertMemoryOption(command, "900m");
  }

  private static void assertMemoryOption(final List<String> command, final String memoryValue) {
    int numMemoryEntries = 0;
    for (final String entry : command) {
      if (entry.startsWith("-Xmx")) {
        numMemoryEntries++;
        assertTrue("-Xmx set to value", entry.endsWith(memoryValue));
      }
    }
    assertEquals(1, numMemoryEntries);
  }

  @Test
  public void testPermSizeDefaults() {
    final List<String> command = newBuilder()
        .build();
    assertPermSizeSet(command, "128m", "128m");
  }

  @Test
  public void testAddPermSizeOptions() {
    final List<String> command = newBuilder()
        .addOption(" -XX:PermSize=256m ")
        .addOption(" -XX:MaxPermSize=512m    ")
        .build();
    assertPermSizeSet(command, "256m", "512m");
  }

  private static void assertPermSizeSet(final List<String> command, final String permSize, final String maxPermSize) {
    int numPermSizeEntries = 0;
    int numMaxPermSizeEntries = 0;
    for (final String entry : command) {
      if (entry.startsWith("-XX:PermSize")) {
        numPermSizeEntries++;
        assertTrue("-XX:PermSize set to value", entry.endsWith(permSize));
      } else if (entry.startsWith("-XX:MaxPermSize")) {
        numMaxPermSizeEntries++;
        assertTrue("-XX:MaxPermSize set to value", entry.endsWith(maxPermSize));
      }
    }
    assertEquals(1, numPermSizeEntries);
    assertEquals(1, numMaxPermSizeEntries);
  }

  private static JavaLaunchCommandBuilder newBuilder() {
    return new JavaLaunchCommandBuilder().setConfigurationFileName("mockConfigurationFileName");
  }
}
