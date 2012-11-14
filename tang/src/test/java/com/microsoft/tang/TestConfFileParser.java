package com.microsoft.tang;

import java.io.ByteArrayOutputStream;

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
    String in = "java.lang.Object=registered\r\ncom.microsoft.tang.TestConfFileParser=com.microsoft.tang.TestConfFileParser\r\n";
    cb.addConfiguration(in);
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    cb.build().writeConfigurationFile(os);
    String out = os.toString();
    Assert.assertEquals(in, out);
  }

}
