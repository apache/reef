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

import org.apache.reef.tang.ConfigurationBuilder;
import org.apache.reef.tang.ExternalConstructor;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.util.logging.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Test LoggingScope
 */
public class LoggingScopeTest {

  private LoggingScopeFactory logFactory;

  @Before
  public void setUp() throws InjectionException {
    final ConfigurationBuilder b = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(LogLevelName.class, "INFO");

    final Injector i = Tang.Factory.getTang().newInjector(b.build());
    logFactory = i.getInstance(LoggingScopeFactory.class);
  }

  /**
   * Test getNewLoggingScope() in LoggingScopeFactory that injects LoggingScope object
   *
   * @throws Exception
   */
  @Test
  public void testGetNewLoggingScope() throws InjectionException {
    try (final LoggingScope ls = logFactory.getNewLoggingScope("test")) {
       Assert.assertTrue(true);
    }
  }

  /**
   * Test getNewLoggingScope() in LoggingScopeFactory that injects LoggingScope object with param as a parameter
   * @throws InjectionException
   */
  @Test
  public void testGetNewLoggingScopeWithParam() throws InjectionException {
    try (final LoggingScope ls = logFactory.getNewLoggingScope("test first string = {0}, second = {1}", new Object[] { "first", "second" })) {
      Assert.assertTrue(true);
    }
  }

  /**
   * Test calling predefined method in LoggingScopeFactory
   *
   * @throws Exception
   */
  @Test
  public void testLoggingScopeFactory() {
    try (final LoggingScope ls = logFactory.activeContextReceived("test")) {
      Assert.assertTrue(true);
    }
  }

  /**
   * Use default log level in injecting LoggingScopeFactory constructor
   * @throws InjectionException
   */
  @Test
  public void testLoggingScopeFactoryWithDefaultLogLevel() throws InjectionException {
    final Injector i = Tang.Factory.getTang().newInjector(Tang.Factory.getTang().newConfigurationBuilder().build());
    final LoggingScopeFactory logFactory = i.getInstance(LoggingScopeFactory.class);

    try (final LoggingScope ls = logFactory.activeContextReceived("test")) {
      Assert.assertTrue(true);
    }
  }
}
