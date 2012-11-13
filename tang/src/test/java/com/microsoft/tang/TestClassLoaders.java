package com.microsoft.tang;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

import org.junit.Test;

import com.microsoft.tang.exceptions.BindException;

public class TestClassLoaders {
  @Test
  public void testOneJar() throws MalformedURLException,
      ClassNotFoundException, BindException {
    ClassLoader cl = new URLClassLoader(new URL[] { new File(
        "../tang-test-jarA/target/tang-test-jarA-1.0-SNAPSHOT.jar").toURI()
        .toURL() });
    Tang.Factory.getTang().newConfigurationBuilder(cl)
        .register(cl.loadClass("com.example.A"));

  }
}
