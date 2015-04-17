/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.reef.wake.test.remote;

import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.impl.DefaultIdentifierFactory;
import org.apache.reef.wake.impl.LoggingEventHandler;
import org.apache.reef.wake.remote.*;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.remote.impl.DefaultRemoteManagerImplementation;
import org.apache.reef.wake.remote.impl.MultiCodec;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.util.HashMap;
import java.util.Map;

public class RemoteIdentifierFactoryTest {
  @Rule
  public final TestName name = new TestName();

  final String logPrefix = "TEST ";

  @Test
  public void testRemoteIdentifierFactory() {
    System.out.println(logPrefix + name.getMethodName());

    Map<String, Class<? extends Identifier>> typeToIdMap = new HashMap<String, Class<? extends Identifier>>();
    typeToIdMap.put("test", TestRemoteIdentifier.class);
    IdentifierFactory factory = new DefaultIdentifierFactory(typeToIdMap);

    String name = "test://name";
    Identifier id = factory.getNewInstance(name);
    System.out.println(id.toString());

    Assert.assertTrue(id instanceof TestRemoteIdentifier);
  }

  @Test
  public void testRemoteManagerIdentifier() throws Exception {
    final Injector injector = Tang.Factory.getTang().newInjector();
    final LocalAddressProvider localAddressProvider = injector.getInstance(LocalAddressProvider.class);

    final int port = 9100;
    final Map<Class<?>, Codec<?>> clazzToCodecMap = new HashMap<Class<?>, Codec<?>>();
    clazzToCodecMap.put(TestEvent.class, new TestEventCodec());
    final Codec<?> codec = new MultiCodec<Object>(clazzToCodecMap);


    try (final RemoteManager rm = new DefaultRemoteManagerImplementation("TestRemoteManager",
        localAddressProvider.getLocalAddress(), port, codec, new LoggingEventHandler<Throwable>(), false, 1, 10000,
        localAddressProvider)) {
      final RemoteIdentifier id = rm.getMyIdentifier();

      final IdentifierFactory factory = new DefaultIdentifierFactory();
      final Identifier newId = factory.getNewInstance(id.toString());

      Assert.assertEquals(id, newId);
    }
  }

}

