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
import org.apache.reef.io.network.naming.parameters.NameResolverRetryCount;
import org.apache.reef.io.network.naming.parameters.NameResolverRetryTimeout;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;

public class NameClientTest {

  private final LocalAddressProvider localAddressProvider;

  public NameClientTest() throws InjectionException {
    this.localAddressProvider = Tang.Factory.getTang().newInjector().getInstance(LocalAddressProvider.class);
  }

  private static final int RETRY_COUNT, RETRY_TIMEOUT;

  static {
    final Tang tang = Tang.Factory.getTang();
    try {
      RETRY_COUNT = tang.newInjector().getNamedInstance(NameResolverRetryCount.class);
      RETRY_TIMEOUT = tang.newInjector().getNamedInstance(NameResolverRetryTimeout.class);
    } catch (final InjectionException e1) {
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
   * Test method for {@link org.apache.reef.io.network.naming.NameClient#close()}.
   *
   * @throws Exception
   */
  @Test
  public final void testClose() throws Exception {
    final String localAddress = localAddressProvider.getLocalAddress();
    final IdentifierFactory factory = new StringIdentifierFactory();
    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileParameter(NameServerParameters.NameServerIdentifierFactory.class, factory);
    injector.bindVolatileInstance(LocalAddressProvider.class, this.localAddressProvider);

    try (final NameServer server = injector.getInstance(NameServer.class)) {
      final int serverPort = server.getPort();
      final Configuration nameResolverConf = NameResolverConfiguration.CONF
          .set(NameResolverConfiguration.NAME_SERVER_HOSTNAME, localAddress)
          .set(NameResolverConfiguration.NAME_SERVICE_PORT, serverPort)
          .set(NameResolverConfiguration.CACHE_TIMEOUT, 10000)
          .set(NameResolverConfiguration.RETRY_TIMEOUT, RETRY_TIMEOUT)
          .set(NameResolverConfiguration.RETRY_COUNT, RETRY_COUNT)
          .build();

      try (final NameResolver client =
               Tang.Factory.getTang().newInjector(nameResolverConf).getInstance(NameClient.class)) {
        final Identifier id = factory.getNewInstance("Task1");
        client.register(id, new InetSocketAddress(localAddress, 7001));
        client.unregister(id);
        Thread.sleep(100);
      }
    }
  }

  /**
   * Test method for {@link org.apache.reef.io.network.naming.NameClient#lookup()}.
   * To check caching behavior with expireAfterAccess & expireAfterWrite
   * Changing NameCache's pattern to expireAfterAccess causes this test to fail
   *
   * @throws Exception
   */
  @Test
  public final void testLookup() throws Exception {
    final String localAddress = localAddressProvider.getLocalAddress();
    final IdentifierFactory factory = new StringIdentifierFactory();
    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileParameter(NameServerParameters.NameServerIdentifierFactory.class, factory);
    injector.bindVolatileInstance(LocalAddressProvider.class, this.localAddressProvider);

    try (final NameServer server = injector.getInstance(NameServer.class)) {
      final int serverPort = server.getPort();
      final Configuration nameResolverConf = NameResolverConfiguration.CONF
          .set(NameResolverConfiguration.NAME_SERVER_HOSTNAME, localAddress)
          .set(NameResolverConfiguration.NAME_SERVICE_PORT, serverPort)
          .set(NameResolverConfiguration.CACHE_TIMEOUT, 150)
          .set(NameResolverConfiguration.RETRY_TIMEOUT, RETRY_TIMEOUT)
          .set(NameResolverConfiguration.RETRY_COUNT, RETRY_COUNT)
          .build();

      try (final NameResolver client =
               Tang.Factory.getTang().newInjector(nameResolverConf).getInstance(NameClient.class)) {
        final Identifier id = factory.getNewInstance("Task1");
        client.register(id, new InetSocketAddress(localAddress, 7001));
        client.lookup(id); // caches the entry
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

}
