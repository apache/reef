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
package org.apache.reef.tang.formats;

import org.apache.commons.cli.ParseException;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.ConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Tests for the CommandLine class.
 */
public final class TestCommandLine {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testNoShortNameToRegister() throws BindException {
    thrown.expect(BindException.class);
    final ConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    final CommandLine cl = new CommandLine(cb);
    cl.registerShortNameOfClass(NamedParameters.StringNoShortNameNoDefault.class);
  }

  /**
   * Tests for parseToConfiguration() with a named parameter that is set.
   *
   * @throws ParseException
   * @throws InjectionException
   */
  @Test
  public void testParseToConfiguration() throws ParseException, InjectionException {
    final String expected = "hello";
    final String[] args = {"-" + NamedParameters.StringShortNameDefault.SHORT_NAME, expected};
    final Configuration configuration = CommandLine
        .parseToConfiguration(args, NamedParameters.StringShortNameDefault.class);
    final String injected = Tang.Factory.getTang().newInjector(configuration)
        .getNamedInstance(NamedParameters.StringShortNameDefault.class);
    Assert.assertEquals(expected, injected);
  }

  /**
   * Tests for parseToConfiguration() with a named parameter that is not set.
   *
   * @throws ParseException
   * @throws InjectionException
   */
  @Test
  public void testParseToConfigurationWithDefault() throws ParseException, InjectionException {
    final Configuration configuration = CommandLine
        .parseToConfiguration(new String[0], NamedParameters.StringShortNameDefault.class);
    final String injected = Tang.Factory.getTang().newInjector(configuration)
        .getNamedInstance(NamedParameters.StringShortNameDefault.class);
    Assert.assertEquals(NamedParameters.StringShortNameDefault.DEFAULT_VALUE, injected);
  }

}
