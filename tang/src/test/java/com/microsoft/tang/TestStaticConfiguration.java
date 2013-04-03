package com.microsoft.tang;

import javax.inject.Inject;

import org.junit.Test;

import com.microsoft.tang.formats.StaticConfiguration;

import static com.microsoft.tang.formats.StaticConfiguration.*;

public class TestStaticConfiguration {
  /**
   * This unit test tries to crash the JVM by creating a cycle in which the a
   * static method body for A calls Class.forName(B), and vice versa.
   * 
   * The injectable constructors are there to get tang to poke around in the
   * class (to defeat lazy initialization schemes).
   */
  @Test
  public void testCyclicBootStrap() {
    @SuppressWarnings("unused")
    StaticConfiguration a = A.conf;
    @SuppressWarnings("unused")
    StaticConfiguration b = B.conf;
  }

  static class A {
    @Inject
    A(B woo, A woo2) {
    }

    static final StaticConfiguration conf = new StaticConfiguration(
        new Bind(B.class, B.class));
  }

  static class B {
    @Inject
    B(B woo, A woo2) {
    }

    static final StaticConfiguration conf = new StaticConfiguration(
        new Bind(A.class, A.class));
  }
}
