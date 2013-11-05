package com.microsoft.tang.formats;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.microsoft.tang.ConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.formats.CommandLine;

public class TestCommandLine {
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testNoShortNameToRegister() throws BindException {
    thrown.expect(BindException.class);
    thrown.expectMessage("Can't register non-existent short name of named parameter: com.microsoft.tang.formats.FooName");
    ConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    CommandLine cl = new CommandLine(cb);
    cl.registerShortNameOfClass(FooName.class);
  }
  
}
@NamedParameter
class FooName implements Name<String> { }