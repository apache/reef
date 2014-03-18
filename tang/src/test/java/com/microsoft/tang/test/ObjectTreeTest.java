package com.microsoft.tang.test;

import com.microsoft.tang.Configuration;
import com.microsoft.tang.Injector;
import com.microsoft.tang.Tang;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

public class ObjectTreeTest {

  @Test
  public void testInstantiation() throws BindException, InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector(getConfiguration());
    final RootInterface root = injector.getInstance(RootInterface.class);
    Assert.assertTrue(root.isValid());
  }


  public Configuration getConfiguration() throws BindException {
    return TestConfiguration.CONF
        .set(TestConfiguration.OPTIONAL_STRING, TestConfiguration.OPTIONAL_STRING_VALUE)
        .set(TestConfiguration.REQUIRED_STRING, TestConfiguration.REQUIRED_STRING_VALUE)
        .build();
  }
}
