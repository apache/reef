/**
 * Copyright (C) 2014 Microsoft Corporation
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
package org.apache.reef.tang.formats;

import junit.framework.Assert;
import org.apache.commons.cli.ParseException;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.ConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class CommandLineTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testNoShortNameToRegister() throws BindException {
    thrown.expect(BindException.class);
    thrown.expectMessage("Can't register non-existent short name of named parameter: org.apache.reef.tang.formats.FooName");
    ConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    CommandLine cl = new CommandLine(cb);
    cl.registerShortNameOfClass(FooName.class);
  }

  /**
   * Tests for parseToConfiguration() with a named parameter that is set
   *
   * @throws ParseException
   * @throws InjectionException
   */
  @Test
  public void testParseToConfiguration() throws ParseException, InjectionException {
    final String expected = "hello";
    final String[] args = {"-" + NamedParameters.AString.SHORT_NAME, expected};
    final Configuration configuration = CommandLine.parseToConfiguration(args, NamedParameters.AString.class);
    Assert.assertEquals(expected, Tang.Factory.getTang().newInjector(configuration).getNamedInstance(NamedParameters.AString.class));
  }

  /**
   * Tests for parseToConfiguration() with a named parameter that is not set
   *
   * @throws ParseException
   * @throws InjectionException
   */
  @Test
  public void testParseToConfigurationWithDefault() throws ParseException, InjectionException {
    final Configuration configuration = CommandLine.parseToConfiguration(new String[0], NamedParameters.AString.class);
    Assert.assertEquals(NamedParameters.AString.DEFAULT_VALUE, Tang.Factory.getTang().newInjector(configuration).getNamedInstance(NamedParameters.AString.class));
  }

}

@NamedParameter
class FooName implements Name<String> {
}