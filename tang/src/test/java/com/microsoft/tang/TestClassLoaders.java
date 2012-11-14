package com.microsoft.tang;

import java.io.File;
import java.net.MalformedURLException;

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

    ConfigurationBuilder cbA = t.newConfigurationBuilder(new File(
        "../tang-test-jarA/target/tang-test-jarA-1.0-SNAPSHOT.jar").toURI()
        .toURL());
    ConfigurationBuilder cbB = t.newConfigurationBuilder(new File(
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
    ConfigurationBuilder cbA = t.newConfigurationBuilder(new File(
        "../tang-test-jarAB/target/tang-test-jarAB-1.0-SNAPSHOT.jar").toURI()
        .toURL());
    ConfigurationBuilder cbB = t.newConfigurationBuilder(new File(
        "../tang-test-jarAB/target/tang-test-jarAB-1.0-SNAPSHOT.jar").toURI()
        .toURL());
    cbA.addConfiguration(cbB.build());

    cbA.register("com.example.A");
    cbA.register("com.example.B");

    t.newInjector(cbA.build());
  }
}
