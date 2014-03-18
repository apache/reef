package com.microsoft.tang.test;

import com.microsoft.tang.Configuration;
import com.microsoft.tang.Tang;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

public class ObjectTreeTest {

  @Test
  public void testInstantiation() throws BindException, InjectionException {
    final RootInterface root = Tang.Factory.getTang().newInjector(getConfiguration()).getInstance(RootInterface.class);
    Assert.assertTrue("Object instantiation left us in an inconsistent state.", root.isValid());
  }

  @Test
  public void testTwoInstantiations() throws BindException, InjectionException {
    final RootInterface firstRoot = Tang.Factory.getTang().newInjector(getConfiguration()).getInstance(RootInterface.class);
    final RootInterface secondRoot = Tang.Factory.getTang().newInjector(getConfiguration()).getInstance(RootInterface.class);
    Assert.assertNotSame("Two instantiations of the object tree should not be the same", firstRoot, secondRoot);
    Assert.assertEquals("Two instantiations of the object tree should be equal", firstRoot, secondRoot);
  }


  public static Configuration getConfiguration() throws BindException {
    return TestConfiguration.CONF
        .set(TestConfiguration.OPTIONAL_STRING, TestConfiguration.OPTIONAL_STRING_VALUE)
        .set(TestConfiguration.REQUIRED_STRING, TestConfiguration.REQUIRED_STRING_VALUE)
        .build();
  }
}
