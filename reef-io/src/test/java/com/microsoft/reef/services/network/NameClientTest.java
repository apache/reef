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
package com.microsoft.reef.services.network;

import com.microsoft.reef.io.network.naming.NameCache;
import com.microsoft.reef.io.network.naming.NameClient;
import com.microsoft.reef.io.network.naming.NameLookupClient;
import com.microsoft.reef.io.network.naming.NameServer;
import com.microsoft.reef.io.network.naming.exception.NamingException;
import com.microsoft.reef.io.network.util.StringIdentifierFactory;
import com.microsoft.tang.Tang;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.wake.Identifier;
import com.microsoft.wake.IdentifierFactory;
import com.microsoft.wake.remote.NetUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;

public class NameClientTest {
  
  static int retryCount, retryTimeout;
  static{
    Tang tang = Tang.Factory.getTang();
    try {
      retryCount = tang.newInjector().getNamedInstance(NameLookupClient.RetryCount.class);
      retryTimeout = tang.newInjector().getNamedInstance(NameLookupClient.RetryTimeout.class);
    } catch (InjectionException e1) {
      throw new RuntimeException("Exception while trying to find default values for retryCount & Timeout", e1);
    }
  }

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }

  /**
   * Test method for {@link com.microsoft.reef.io.network.naming.NameClient#close()}.
   *
   * @throws Exception
   */
  @Test
  public final void testClose() throws Exception {
    IdentifierFactory factory = new StringIdentifierFactory();
    try (NameServer server = new NameServer(0, factory)) {
      int serverPort = server.getPort();
      try (NameClient client = new NameClient(NetUtils.getLocalAddress(), serverPort, factory, retryCount, retryTimeout,
          new NameCache(10000))) {
        Identifier id = factory.getNewInstance("Task1");
        client.register(id, new InetSocketAddress(NetUtils.getLocalAddress(), 7001));
        client.unregister(id);
        Thread.sleep(100);
      }
    }
  }

  /**
   * Test method for {@link com.microsoft.reef.io.network.naming.NameClient#lookup()}.
   * To check caching behavior with expireAfterAccess & expireAfterWrite
   * Changing NameCache's pattern to expireAfterAccess causes this test to fail
   *
   * @throws Exception
   */
  @Test
  public final void testLookup() throws Exception {
    IdentifierFactory factory = new StringIdentifierFactory();
    try (NameServer server = new NameServer(0, factory)) {
      int serverPort = server.getPort();
      try (NameClient client = new NameClient(NetUtils.getLocalAddress(), serverPort, factory, retryCount, retryTimeout,
          new NameCache(150))) {
        Identifier id = factory.getNewInstance("Task1");
        client.register(id, new InetSocketAddress(NetUtils.getLocalAddress(), 7001));
        client.lookup(id);// caches the entry
        client.unregister(id);
        Thread.sleep(100);
        try {
          InetSocketAddress addr = client.lookup(id);
          Thread.sleep(100);
          //With expireAfterAccess, the previous lookup would reset expiry to 150ms
          //more and 100ms wait will not expire the item and will return the cached value
          //With expireAfterWrite, the extra wait of 100 ms will expire the item
          //resulting in NamingException and the test passes
          addr = client.lookup(id);
          Assert.assertNull("client.lookup(id)", addr);
        } catch (Exception e) {
          if (e instanceof ExecutionException) {
            Assert.assertTrue("Execution Exception cause is instanceof NamingException", e.getCause() instanceof NamingException);
          } else
            throw e;
        }
      }
    }
  }

}
