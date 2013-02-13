package com.microsoft.tang;

import java.io.File;
import java.net.MalformedURLException;

import junit.framework.Assert;

import org.junit.Test;

import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;

public class TestClassLoaders {
  @Test
  public void testOneJar() throws MalformedURLException,
      ClassNotFoundException, BindException {
    Tang.Factory
        .getTang()
        .newConfigurationBuilder(
            new File("../tang-test-jarA/target/tang-test-jarA-1.0-SNAPSHOT.jar")
                .toURI().toURL()).register("com.example.A");

  }

  @Test
  public void testTwoJars() throws MalformedURLException,
      ClassNotFoundException, BindException, InjectionException {
    Tang t = Tang.Factory.getTang();

    JavaConfigurationBuilder cbA = t.newConfigurationBuilder(new File(
        "../tang-test-jarA/target/tang-test-jarA-1.0-SNAPSHOT.jar").toURI()
        .toURL());
    JavaConfigurationBuilder cbB = t.newConfigurationBuilder(new File(
        "../tang-test-jarB/target/tang-test-jarB-1.0-SNAPSHOT.jar").toURI()
        .toURL());
    cbA.addConfiguration(cbB.build());

    cbA.register("com.example.A");
    cbA.register("com.example.B");

    t.newInjector(cbA.build());
  }

  @Test
  public void testTwoClasses() throws MalformedURLException,
      ClassNotFoundException, BindException, InjectionException {
    Tang t = Tang.Factory.getTang();
    JavaConfigurationBuilder cbA = t.newConfigurationBuilder(new File(
        "../tang-test-jarAB/target/tang-test-jarAB-1.0-SNAPSHOT.jar").toURI()
        .toURL());
    JavaConfigurationBuilder cbB = t.newConfigurationBuilder(new File(
        "../tang-test-jarAB/target/tang-test-jarAB-1.0-SNAPSHOT.jar").toURI()
        .toURL());
    cbA.addConfiguration(cbB.build());

    cbA.register("com.example.A");
    cbA.register("com.example.B");

    t.newInjector(cbA.build());
  }
  // TODO: This test does result in an infeasible plan right now -- Markus
//  @Test
  public void testTwoChildrenOneJarDifferentTypes()
      throws MalformedURLException, InjectionException, BindException,
      ClassNotFoundException {
    Tang t = Tang.Factory.getTang();
    JavaConfigurationBuilder cbA1 = t.newConfigurationBuilder(new File(
        "../tang-test-jarAB/target/tang-test-jarAB-1.0-SNAPSHOT.jar").toURI()
        .toURL());
    JavaConfigurationBuilder cbA2 = t.newConfigurationBuilder(new File(
        "../tang-test-jarAB/target/tang-test-jarAB-1.0-SNAPSHOT.jar").toURI()
        .toURL());
    cbA1.register("com.example.A");
    cbA1.bind("com.example.A", "com.example.B");
    cbA2.bind("com.example.A", "com.example.B");
    Object o = t.newInjector(cbA1.build()).getInstance("com.example.A");
    Object p = t.newInjector(cbA2.build()).getInstance("com.example.A");
    Assert.assertNotSame(o.getClass(), p.getClass());
  }
}
