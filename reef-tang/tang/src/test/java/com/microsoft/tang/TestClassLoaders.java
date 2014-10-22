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
import java.net.MalformedURLException;

import org.junit.Assert;

import org.junit.Test;

import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.exceptions.NameResolutionException;
import com.microsoft.tang.types.ClassNode;

public class TestClassLoaders {

//  @Test
  public void testOneJar() throws MalformedURLException,
      ClassNotFoundException, NameResolutionException, BindException {
    Tang.Factory
        .getTang()
        .newConfigurationBuilder(
            new File("../tang-test-jarA/target/tang-test-jarA-1.0-SNAPSHOT.jar")
                .toURI().toURL()).getClassHierarchy().getNode("com.example.A");

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

    cbA.getClassHierarchy().getNode("com.example.A");
    cbA.getClassHierarchy().getNode("com.example.B");

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

    cbA.getClassHierarchy().getNode("com.example.A");
    cbA.getClassHierarchy().getNode("com.example.B");

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
    cbA1.getClassHierarchy().getNode("com.example.A");
    cbA1.bind("com.example.A", "com.example.B");
    cbA2.bind("com.example.A", "com.example.B");
    Object o = t.newInjector(cbA1.build()).getInstance("com.example.A");
    Object p = t.newInjector(cbA2.build()).getInstance("com.example.A");
    Assert.assertSame(o.getClass(), p.getClass());
    JavaConfigurationBuilder cbAother = t.newConfigurationBuilder(new File(
        "../tang-test-jarA/target/tang-test-jarA-1.0-SNAPSHOT.jar").toURI()
        .toURL());

    Assert.assertEquals(1,((ClassNode<?>)(cbA1.getClassHierarchy().getNode("com.example.A"))).getInjectableConstructors().length);
    Assert.assertEquals(0,((ClassNode<?>)(cbAother.getClassHierarchy().getNode("com.example.A"))).getInjectableConstructors().length);
    
  }
//  @Test
  public void testOneClassOneJar()
      throws MalformedURLException, InjectionException, BindException,
      ClassNotFoundException, NameResolutionException {
    Tang t = Tang.Factory.getTang();
    JavaConfigurationBuilder cbA1 = t.newConfigurationBuilder(new File(
        "../tang-test-jarAB/target/tang-test-jarAB-1.0-SNAPSHOT.jar").toURI()
        .toURL());
    cbA1.bind("com.example.A", "com.example.B");
    cbA1.getClassHierarchy().getNode("com.example.A");
    t.newInjector(cbA1.build()).getInstance("com.example.B");
  }
}
