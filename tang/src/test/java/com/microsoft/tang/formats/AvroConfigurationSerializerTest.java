package com.microsoft.tang.formats;

import com.microsoft.tang.Configuration;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.avro.AvroConfiguration;
import org.junit.Assert;
import org.junit.Test;

import javax.inject.Inject;

/**
 * Tests for the Avro Serializer
 */
public class AvroConfigurationSerializerTest {

  @NamedParameter
  public static final class AnIntParameter implements Name<Integer> {
  }


  @Test
  public void testRoundTrip() throws BindException, InjectionException {
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindImplementation(InterfaceA.class, ImplementationA.class);
    cb.bindNamedParameter(AnIntParameter.class, "5");
    cb.bindImplementation(InterfaceB.class, ImplementationB.class);

    final Configuration conf = cb.build();

    final InterfaceB before = Tang.Factory.getTang().newInjector(conf).getInstance(InterfaceB.class);
    final InterfaceB after = Tang.Factory.getTang().newInjector(roundTrip(conf)).getInstance(InterfaceB.class);

    Assert.assertEquals(before, after);
  }

  private static final Configuration roundTrip(final Configuration conf) throws BindException {
    final AvroConfiguration aConf = new AvroConfigurationSerializer().toAvro(conf);
    return new AvroConfigurationSerializer().fromAvro(aConf);
  }

}

interface InterfaceA {
}

class ImplementationA implements InterfaceA {
  private final int theInteger;

  @Inject
  public ImplementationA(@Parameter(AvroConfigurationSerializerTest.AnIntParameter.class) final int theInt) {
    this.theInteger = theInt;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ImplementationA that = (ImplementationA) o;

    if (theInteger != that.theInteger) return false;

    return true;
  }

  @Override
  public int hashCode() {
    return theInteger;
  }
}

interface InterfaceB {
}

class ImplementationB implements InterfaceB {
  private final InterfaceA interfaceA;

  @Inject
  public ImplementationB(final InterfaceA interfaceA) {
    this.interfaceA = interfaceA;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ImplementationB that = (ImplementationB) o;

    if (interfaceA != null ? !interfaceA.equals(that.interfaceA) : that.interfaceA != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    return interfaceA != null ? interfaceA.hashCode() : 0;
  }
}
