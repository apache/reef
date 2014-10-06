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
package com.microsoft.tang.test;

import com.microsoft.tang.ClassHierarchy;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Tang;
import com.microsoft.tang.implementation.protobuf.ProtocolBufferClassHierarchy;
import org.junit.Assert;
import org.junit.Test;

/**
 * Base class for roundtrip tests. The idea is that serializers implement roundTrip() and then get tested by the tests
 * in this class.
 */
public abstract class RoundTripTest {

  public abstract Configuration roundTrip(final Configuration configuration) throws Exception;
  public abstract Configuration roundTrip(final Configuration configuration, final ClassHierarchy classHierarchy) throws Exception;

  @Test
  public void testRoundTrip() throws Exception {
    // TODO: use 'getConfiguration' instead of 'getConfigurationWithoutList' after #192 is fixed
    final Configuration conf = ObjectTreeTest.getConfigurationWithoutList();
    final RootInterface before = Tang.Factory.getTang().newInjector(conf).getInstance(RootInterface.class);
    final RootInterface after = Tang.Factory.getTang().newInjector(roundTrip(conf)).getInstance(RootInterface.class);
    Assert.assertEquals("Configuration conversion to and from Avro datatypes failed.", before, after);
  }

  @Test
  public void testRoundTripWithClassHierarchy() throws Exception {
    // TODO: use 'getConfiguration' instead of 'getConfigurationWithoutList' after #192 is fixed
    final Configuration confBefore = ObjectTreeTest.getConfigurationWithoutList();
    final ClassHierarchy c = new ProtocolBufferClassHierarchy(ProtocolBufferClassHierarchy.serialize(confBefore.getClassHierarchy()));
    final Configuration confAfter = roundTrip(confBefore, c);
    Assert.assertEquals(confBefore.getNamedParameters().size(), confAfter.getNamedParameters().size());
    //For now, we cannot use ProtocolBufferClassHierarchy to do injection
  }
}
