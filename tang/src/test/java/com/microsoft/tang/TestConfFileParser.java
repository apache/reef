package com.microsoft.tang;

import java.io.ByteArrayOutputStream;

import javax.inject.Inject;

import junit.framework.Assert;

import org.junit.Test;

import com.microsoft.tang.exceptions.BindException;

public class TestConfFileParser {

  @Test
  public void testRoundTrip() throws BindException {
    // TODO: This likely only passes on windows, as it relies on newlines
    // being \r\n, and on java.lang.Object having a lower hash code than
    // com.microsoft.tang.TestConfFileParser
    Tang t = Tang.Factory.getTang();
    ConfigurationBuilder cb = t.newConfigurationBuilder();
    String in = "com.microsoft.tang.TestConfFileParser=com.microsoft.tang.TestConfFileParser\r\n";
    cb.addConfiguration(in);
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    cb.build().writeConfigurationFile(os);
    String out = os.toString();
    Assert.assertEquals(in, out);
  }
  @Test
  public void testBindSingleton() throws BindException {
    // TODO: This likely only passes on windows, as it relies on newlines
    // being \r\n, and on java.lang.Object having a lower hash code than
    // com.microsoft.tang.TestConfFileParser
    Tang t = Tang.Factory.getTang();
    ConfigurationBuilder cb = t.newConfigurationBuilder();
    //cb.bindSingletonImplementation(SingleTest.A.class, SingleTest.B.class);
    cb.bindSingleton(SingleTest.A.class);
    cb.bindImplementation(SingleTest.A.class, SingleTest.B.class);
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    cb.build().writeConfigurationFile(os);
    String out = os.toString();
    String in = "com.microsoft.tang.SingleTest$A=com.microsoft.tang.SingleTest$B\r\ncom.microsoft.tang.SingleTest$A=singleton\r\n";
    Assert.assertEquals(in, out);
  }
}
class SingleTest {
  static class A{}
  static class B extends A{ @Inject B() {} }
}
