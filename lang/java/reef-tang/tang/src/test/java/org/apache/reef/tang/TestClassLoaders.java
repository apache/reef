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

import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.exceptions.NameResolutionException;
import org.apache.reef.tang.types.ClassNode;
import org.junit.Assert;

import java.io.File;
import java.net.MalformedURLException;

public class TestClassLoaders {

  //  @Test
  public void testOneJar() throws MalformedURLException,
      ClassNotFoundException, NameResolutionException, BindException {
    Tang.Factory
        .getTang()
        .newConfigurationBuilder(
            new File("../tang-test-jarA/target/tang-test-jarA-1.0-SNAPSHOT.jar")
                .toURI().toURL()).getClassHierarchy().getNode("org.apache.reef.tang.examples.A");

  }

  //  @Test
  public void testTwoJars() throws MalformedURLException,
      ClassNotFoundException, BindException, InjectionException, NameResolutionException {
    Tang t = Tang.Factory.getTang();

    JavaConfigurationBuilder cbA = t.newConfigurationBuilder(new File(
        "../tang-test-jarA/target/tang-test-jarA-1.0-SNAPSHOT.jar").toURI()
        .toURL());
    JavaConfigurationBuilder cbB = t.newConfigurationBuilder(new File(
        "../tang-test-jarB/target/tang-test-jarB-1.0-SNAPSHOT.jar").toURI()
        .toURL());
    cbA.addConfiguration(cbB.build());

    cbA.getClassHierarchy().getNode("org.apache.reef.tang.examples.A");
    cbA.getClassHierarchy().getNode("org.apache.reef.tang.examples.B");

    t.newInjector(cbA.build());
  }

  //  @Test
  public void testTwoClasses() throws MalformedURLException,
      ClassNotFoundException, BindException, InjectionException, NameResolutionException {
    Tang t = Tang.Factory.getTang();
    JavaConfigurationBuilder cbA = t.newConfigurationBuilder(new File(
        "../tang-test-jarAB/target/tang-test-jarAB-1.0-SNAPSHOT.jar").toURI()
        .toURL());
    JavaConfigurationBuilder cbB = t.newConfigurationBuilder(new File(
        "../tang-test-jarAB/target/tang-test-jarAB-1.0-SNAPSHOT.jar").toURI()
        .toURL());
    cbA.addConfiguration(cbB.build());

    cbA.getClassHierarchy().getNode("org.apache.reef.tang.examples.A");
    cbA.getClassHierarchy().getNode("org.apache.reef.tang.examples.B");

    t.newInjector(cbA.build());
  }

  //  @Test
  public void aliasingNameSameDifferentTypes()
      throws MalformedURLException, InjectionException, BindException,
      ClassNotFoundException, NameResolutionException {
    Tang t = Tang.Factory.getTang();
    JavaConfigurationBuilder cbA1 = t.newConfigurationBuilder(new File(
        "../tang-test-jarAB/target/tang-test-jarAB-1.0-SNAPSHOT.jar").toURI()
        .toURL());
    JavaConfigurationBuilder cbA2 = t.newConfigurationBuilder(new File(
        "../tang-test-jarAB/target/tang-test-jarAB-1.0-SNAPSHOT.jar").toURI()
        .toURL());
    cbA1.getClassHierarchy().getNode("org.apache.reef.tang.examples.A");
    cbA1.bind("org.apache.reef.tang.examples.A", "org.apache.reef.tang.examples.B");
    cbA2.bind("org.apache.reef.tang.examples.A", "org.apache.reef.tang.examples.B");
    Object o = t.newInjector(cbA1.build()).getInstance("org.apache.reef.tang.examples.A");
    Object p = t.newInjector(cbA2.build()).getInstance("org.apache.reef.tang.examples.A");
    Assert.assertSame(o.getClass(), p.getClass());
    JavaConfigurationBuilder cbAother = t.newConfigurationBuilder(new File(
        "../tang-test-jarA/target/tang-test-jarA-1.0-SNAPSHOT.jar").toURI()
        .toURL());

    Assert.assertEquals(1, ((ClassNode<?>) (cbA1.getClassHierarchy().getNode("org.apache.reef.tang.examples.A"))).getInjectableConstructors().length);
    Assert.assertEquals(0, ((ClassNode<?>) (cbAother.getClassHierarchy().getNode("org.apache.reef.tang.examples.A"))).getInjectableConstructors().length);

  }

  //  @Test
  public void testOneClassOneJar()
      throws MalformedURLException, InjectionException, BindException,
      ClassNotFoundException, NameResolutionException {
    Tang t = Tang.Factory.getTang();
    JavaConfigurationBuilder cbA1 = t.newConfigurationBuilder(new File(
        "../tang-test-jarAB/target/tang-test-jarAB-1.0-SNAPSHOT.jar").toURI()
        .toURL());
    cbA1.bind("org.apache.reef.tang.examples.A", "org.apache.reef.tang.examples.B");
    cbA1.getClassHierarchy().getNode("org.apache.reef.tang.examples.A");
    t.newInjector(cbA1.build()).getInstance("org.apache.reef.tang.examples.B");
  }
}
