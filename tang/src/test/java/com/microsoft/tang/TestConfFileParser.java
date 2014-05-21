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
package com.microsoft.tang;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import javax.inject.Inject;

import org.junit.Assert;

import org.junit.Before;
import org.junit.Test;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.ConfigurationFile;
import com.microsoft.tang.implementation.TangImpl;
import com.microsoft.tang.util.ReflectionUtilities;

public class TestConfFileParser {

  @Before
  public void setUp() {
    TangImpl.reset();
  }
  
  @Test
  public void testRoundTrip() throws BindException {
    // TODO: This likely only passes on windows, as it relies on newlines
    // being \r\n, and on java.lang.Object having a lower hash code than
    // com.microsoft.tang.TestConfFileParser
    Tang t = Tang.Factory.getTang();
    JavaConfigurationBuilder cb = t.newConfigurationBuilder();
    String in = "com.microsoft.tang.TestConfFileParser=com.microsoft.tang.TestConfFileParser\n";
    ConfigurationFile.addConfiguration(cb, in);
    String out = ConfigurationFile.toConfigurationString(cb.build());
    Assert.assertEquals(in, out);
  }
  @Test
  public void testBindSingleton() throws BindException {
    // TODO: This likely only passes on windows, as it relies on newlines
    // being \r\n, and on java.lang.Object having a lower hash code than
    // com.microsoft.tang.TestConfFileParser
    Tang t = Tang.Factory.getTang();
    JavaConfigurationBuilder cb = t.newConfigurationBuilder();
    cb.bindImplementation(SingleTest.A.class, SingleTest.B.class);
    
    String out = ConfigurationFile.toConfigurationString(cb.build());
    String in = "com.microsoft.tang.SingleTest$A=com.microsoft.tang.SingleTest$B\n";
    Assert.assertEquals(in, out);
  }
  
  @Test
  public void testNamedParameter() throws BindException {
    Tang t = Tang.Factory.getTang();
    String conf = "com.microsoft.tang.TestConfFileParser$Foo=woot";
    ConfigurationBuilder cb = t.newConfigurationBuilder();
    ConfigurationFile.addConfiguration(cb, conf);
    Assert.assertTrue(t.newInjector(cb.build()).isParameterSet(Foo.class));
  }
  
  @NamedParameter()
  class Foo implements Name<String> { }
  
  @NamedParameter(doc = "remote id.")
  private final static class RemoteIdentifier implements Name<String> { }
  
  @Test
  public void testNamedParameter2() throws BindException, IOException, InjectionException {

    final String value = "socket://131.179.176.216:19278";
    final File tmp = File.createTempFile("test", "conf");

    try (final FileOutputStream fout = new FileOutputStream(tmp)) {
      final String line = ReflectionUtilities.getFullName(RemoteIdentifier.class) + "=" + value;
      fout.write(line.getBytes());
    }

    JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    ConfigurationFile.addConfiguration(cb, tmp);
    final Injector i = Tang.Factory.getTang().newInjector(cb.build());
    Assert.assertEquals(value, i.getNamedInstance(RemoteIdentifier.class));
  }
}

@NamedParameter
final class Foo implements Name<String> {
}
class SingleTest {
  static class A{}
  static class B extends A{ @Inject B() {} }
}
