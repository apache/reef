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
package com.microsoft.wake.test.remote;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import com.microsoft.wake.Identifier;
import com.microsoft.wake.IdentifierFactory;
import com.microsoft.wake.impl.DefaultIdentifierFactory;
import com.microsoft.wake.impl.LoggingEventHandler;
import com.microsoft.wake.remote.Codec;
import com.microsoft.wake.remote.NetUtils;
import com.microsoft.wake.remote.RemoteIdentifier;
import com.microsoft.wake.remote.RemoteManager;
import com.microsoft.wake.remote.impl.DefaultRemoteManagerImplementation;
import com.microsoft.wake.remote.impl.MultiCodec;

public class RemoteIdentifierFactoryTest {
  @Rule public final TestName name = new TestName();

  final String logPrefix = "TEST ";

  @Test
  public void testRemoteIdentifierFactory() {
    System.out.println(logPrefix + name.getMethodName());

    Map<String, Class<? extends Identifier>> typeToIdMap = new HashMap<String, Class<? extends Identifier>> ();
    typeToIdMap.put("test", TestRemoteIdentifier.class);
    IdentifierFactory factory = new DefaultIdentifierFactory(typeToIdMap);
    
    String name = "test://name";
    Identifier id = factory.getNewInstance(name);
    System.out.println(id.toString());
    
    Assert.assertTrue(id instanceof TestRemoteIdentifier);
  }
   
  @Test
  public void testRemoteManagerIdentifier() throws Exception {
    System.out.println(logPrefix + name.getMethodName());
    
    int port = 9100;
    Map<Class<?>, Codec<?>> clazzToCodecMap = new HashMap<Class<?>, Codec<?>>();
    clazzToCodecMap.put(TestEvent.class, new TestEventCodec());
    Codec<?> codec = new MultiCodec<Object>(clazzToCodecMap);
    
    String hostAddress = NetUtils.getLocalAddress();
    
    RemoteManager rm = new DefaultRemoteManagerImplementation("TestRemoteManager", 
        hostAddress, port, codec, new LoggingEventHandler<Throwable> (), false, 1, 10000);
    RemoteIdentifier id = rm.getMyIdentifier();
    System.out.println(id.toString());
    
    IdentifierFactory factory = new DefaultIdentifierFactory();
    Identifier newid = factory.getNewInstance(id.toString());
    System.out.println(newid.toString());
    
    Assert.assertTrue(id.equals(newid));
    
    rm.close();
  }

}

