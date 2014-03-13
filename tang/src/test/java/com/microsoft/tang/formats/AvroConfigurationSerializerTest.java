/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.tang.formats;

import com.microsoft.tang.Configuration;
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
import java.io.File;
import java.io.IOException;

/**
 * Tests for the Avro Serializer
 */
public class AvroConfigurationSerializerTest {

  @NamedParameter
  public static final class AnIntParameter implements Name<Integer> {
  }

  /**
   * Tests the roundtrip of a Configuration to and from Avro.
   *
   * @throws BindException
   * @throws InjectionException
   */
  @Test
  public void testRoundTrip() throws BindException, InjectionException {
    final Configuration conf = getRoundTripConfiguration();
    final InterfaceB before = Tang.Factory.getTang().newInjector(conf).getInstance(InterfaceB.class);
    final InterfaceB after = Tang.Factory.getTang().newInjector(roundTripThroughAvro(conf)).getInstance(InterfaceB.class);
    Assert.assertEquals("Configuration conversion to and from Avro datatypes failed.", before, after);
  }

  /**
   * Tests the roundtrip of a Configuration to and from an Avro file.
   *
   * @throws BindException
   * @throws InjectionException
   */
  @Test
  public void testRoundTripThroughFile() throws BindException, InjectionException, IOException {
    final Configuration conf = getRoundTripConfiguration();
    final InterfaceB before = Tang.Factory.getTang().newInjector(conf).getInstance(InterfaceB.class);
    final InterfaceB after = Tang.Factory.getTang().newInjector(roundTripThroughFile(conf)).getInstance(InterfaceB.class);
    Assert.assertEquals("Configuration (de-)serialization to a file failed.", before, after);
  }

  /**
   * Tests the roundtrip of a Configuration to and from a byte[].
   *
   * @throws BindException
   * @throws InjectionException
   */
  @Test
  public void testRoundTripThroughByteArray() throws BindException, InjectionException, IOException {
    final Configuration conf = getRoundTripConfiguration();
    final InterfaceB before = Tang.Factory.getTang().newInjector(conf).getInstance(InterfaceB.class);
    final InterfaceB after = Tang.Factory.getTang().newInjector(roundtripThroughByteArray(conf)).getInstance(InterfaceB.class);
    Assert.assertEquals("Configuration (de-)serialization to a byte[] failed.", before, after);
  }

  /**
   * @return The Configuration used in the roundtrip tests.
   * @throws BindException
   */
  private Configuration getRoundTripConfiguration() throws BindException {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(InterfaceA.class, ImplementationA.class)
        .bindNamedParameter(AnIntParameter.class, "5")
        .bindImplementation(InterfaceB.class, ImplementationB.class)
        .build();
  }

  /**
   * Converts the given Configuration to AvroConfiguration and back.
   *
   * @param conf
   * @return
   * @throws BindException
   */
  private Configuration roundTripThroughAvro(final Configuration conf) throws BindException {
    final AvroConfiguration aConf = new AvroConfigurationSerializer().toAvro(conf);
    return new AvroConfigurationSerializer().fromAvro(aConf);
  }

  /**
   * Stores the given Configuration in an Avro file and reads it back.
   *
   * @param conf
   * @return
   * @throws BindException
   * @throws IOException
   */
  private Configuration roundTripThroughFile(final Configuration conf) throws BindException, IOException {
    final File tempFile = File.createTempFile("TangTest", "avroconf");
    final AvroConfigurationSerializer serializer = new AvroConfigurationSerializer();
    serializer.toFile(conf, tempFile);
    return serializer.fromFile(tempFile);
  }

  private Configuration roundtripThroughByteArray(final Configuration conf) throws IOException, BindException {
    final AvroConfigurationSerializer serializer = new AvroConfigurationSerializer();
    final byte[] theBytes = serializer.toByteArray(conf);
    return serializer.fromByteArray(theBytes);
  }

}

// Below: The classes used to form the object graph we are testing with.

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
