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
package org.apache.reef.io.network.naming;

import org.apache.reef.io.network.naming.exception.NamingException;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;

public class LocalNameResolverTest {

  private final LocalAddressProvider localAddressProvider;

  public LocalNameResolverTest() throws InjectionException {
    this.localAddressProvider = Tang.Factory.getTang().newInjector().getInstance(LocalAddressProvider.class);
  }

  /**
   * Test method for {@link org.apache.reef.io.network.naming.LocalNameResolverImpl#close()}.
   *
   * @throws Exception
   */
  @Test
  public final void testClose() throws Exception {
    final String localAddress = localAddressProvider.getLocalAddress();
    final IdentifierFactory factory = new StringIdentifierFactory();
    try (final NameResolver resolver = Tang.Factory.getTang().newInjector(LocalNameResolverConfiguration.CONF
        .set(LocalNameResolverConfiguration.CACHE_TIMEOUT, 10000)
        .build()).getInstance(NameResolver.class)) {
      final Identifier id = factory.getNewInstance("Task1");
      resolver.register(id, new InetSocketAddress(localAddress, 7001));
      resolver.unregister(id);
      Thread.sleep(100);
    }
  }

  /**
   * Test method for {@link org.apache.reef.io.network.naming.LocalNameResolverImpl#lookup(Identifier id)}.
   * To check caching behavior with expireAfterAccess & expireAfterWrite
   * Changing NameCache's pattern to expireAfterAccess causes this test to fail
   *
   * @throws Exception
   */
  @Test
  public final void testLookup() throws Exception {
    final IdentifierFactory factory = new StringIdentifierFactory();
    final String localAddress = localAddressProvider.getLocalAddress();
    try (final NameResolver resolver = Tang.Factory.getTang().newInjector(LocalNameResolverConfiguration.CONF
        .set(LocalNameResolverConfiguration.CACHE_TIMEOUT, 150)
        .build()).getInstance(NameResolver.class)) {
      final Identifier id = factory.getNewInstance("Task1");
      final InetSocketAddress socketAddr = new InetSocketAddress(localAddress, 7001);
      resolver.register(id, socketAddr);
      InetSocketAddress lookupAddr = resolver.lookup(id); // caches the entry
      Assert.assertTrue(socketAddr.equals(lookupAddr));
      resolver.unregister(id);
      Thread.sleep(100);
      try {
        lookupAddr = resolver.lookup(id);
        Thread.sleep(100);
        //With expireAfterAccess, the previous lookup would reset expiry to 150ms
        //more and 100ms wait will not expire the item and will return the cached value
        //With expireAfterWrite, the extra wait of 100 ms will expire the item
        //resulting in NamingException and the test passes
        lookupAddr = resolver.lookup(id);
        Assert.assertNull("resolver.lookup(id)", lookupAddr);
      } catch (final Exception e) {
        if (e instanceof ExecutionException) {
          Assert.assertTrue("Execution Exception cause is instanceof NamingException",
              e.getCause() instanceof NamingException);
        } else {
          throw e;
        }
      }
    }
  }
}
