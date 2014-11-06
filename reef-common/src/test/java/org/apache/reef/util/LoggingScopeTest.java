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

package org.apache.reef.util;

import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.util.logging.LoggingScope;
import org.apache.reef.util.logging.LoggingScopeFactory;
import org.apache.reef.util.logging.ReefLoggingScope;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.logging.Logger;

/**
 * Test LoggingScope
 */
public class LoggingScopeTest {

  LoggingScopeFactory logFactory;

  @Before
  public void setUp() throws Exception {
    logFactory = Tang.Factory.getTang().newInjector(Tang.Factory.getTang().newConfigurationBuilder().build()).getInstance(LoggingScopeFactory.class);
  }

  /**
   * test getLoggingScope() in LoggingScopeFactory that injects  LoggingScope object
   *
   * @throws Exception
   */
  @Test
  public void testInjectedLoogingScope() throws Exception {
    try (LoggingScope ls = logFactory.getLoggingScope("test"))
    {
       Assert.assertTrue(true);
    }
  }

  /**
   * Test creating ReefLoggingScope object directly
   * @throws Exception
   */
  @Test
  public void testNewLoogingScope() throws Exception {
    try (LoggingScope ls = new ReefLoggingScope(Logger.getLogger(LoggingScopeFactory.class.getName()), "test"))
    {
      Assert.assertTrue(true);
    }
  }

  /**
   *  Test creating ReefLoggingScope object with params
   * @throws Exception
   */
  @Test
  public void testNewLoogingScopeConstructorWithParameters() throws Exception {
    try (LoggingScope ls = new ReefLoggingScope(Logger.getLogger(LoggingScopeFactory.class.getName()), "test first string = {0}, second = {1}", new Object[] { "first", "second" }))
    {
      Assert.assertTrue(true);
    }
  }

  /**
   * Test calling predefined method in LoggingScopeFactory
   *
   * @throws Exception
   */
  @Test
  public void testLoogingScopeFactgory() throws Exception {
    try (LoggingScope ls = logFactory.activeContextReceived("test"))
    {
      Assert.assertTrue(true);
    }
  }
}
