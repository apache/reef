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
package org.apache.reef.tang;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.ConfigurationFile;
import org.apache.reef.tang.implementation.TangImpl;
import org.apache.reef.tang.util.ReflectionUtilities;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.inject.Inject;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class TestConfFileParser {

  @Before
  public void setUp() {
    TangImpl.reset();
  }

  @Test
  public void testRoundTrip() throws BindException {
    // TODO: This likely only passes on windows, as it relies on newlines
    // being \r\n, and on java.lang.Object having a lower hash code than
    // org.apache.reef.tang.TestConfFileParser
    Tang t = Tang.Factory.getTang();
    JavaConfigurationBuilder cb = t.newConfigurationBuilder();
    String in = "org.apache.reef.tang.TestConfFileParser=org.apache.reef.tang.TestConfFileParser\n";
    ConfigurationFile.addConfiguration(cb, in);
    String out = ConfigurationFile.toConfigurationString(cb.build());
    Assert.assertEquals(in, out);
  }

  @Test
  public void testBindSingleton() throws BindException {
    // TODO: This likely only passes on windows, as it relies on newlines
    // being \r\n, and on java.lang.Object having a lower hash code than
    // org.apache.reef.tang.TestConfFileParser
    Tang t = Tang.Factory.getTang();
    JavaConfigurationBuilder cb = t.newConfigurationBuilder();
    cb.bindImplementation(SingleTest.A.class, SingleTest.B.class);

    String out = ConfigurationFile.toConfigurationString(cb.build());
    String in = "org.apache.reef.tang.SingleTest$A=org.apache.reef.tang.SingleTest$B\n";
    Assert.assertEquals(in, out);
  }

  @Test
  public void testNamedParameter() throws BindException {
    Tang t = Tang.Factory.getTang();
    String conf = "org.apache.reef.tang.TestConfFileParser$Foo=woot";
    ConfigurationBuilder cb = t.newConfigurationBuilder();
    ConfigurationFile.addConfiguration(cb, conf);
    Assert.assertTrue(t.newInjector(cb.build()).isParameterSet(Foo.class));
  }

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

  @NamedParameter(doc = "remote id.")
  private final static class RemoteIdentifier implements Name<String> {
  }

  @NamedParameter()
  class Foo implements Name<String> {
  }
}

@NamedParameter
final class Foo implements Name<String> {
}

class SingleTest {
  static class A {
  }

  static class B extends A {
    @Inject
    B() {
    }
  }
}
