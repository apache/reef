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
package com.microsoft.tang.formats;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.microsoft.tang.ConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.formats.CommandLine;

public class TestCommandLine {
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testNoShortNameToRegister() throws BindException {
    thrown.expect(BindException.class);
    thrown.expectMessage("Can't register non-existent short name of named parameter: com.microsoft.tang.formats.FooName");
    ConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    CommandLine cl = new CommandLine(cb);
    cl.registerShortNameOfClass(FooName.class);
  }
  
}
@NamedParameter
class FooName implements Name<String> { }