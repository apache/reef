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

import org.apache.reef.io.naming.NameAssignment;
import org.apache.reef.io.network.naming.parameters.*;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Naming server and client test.
 */
public class NamingTest {

  private static final Logger LOG = Logger.getLogger(NamingTest.class.getName());
  private static final int RETRY_COUNT;
  private static final int RETRY_TIMEOUT;

  static {
    try {
      final Injector injector = Tang.Factory.getTang().newInjector();
      RETRY_COUNT = injector.getNamedInstance(NameResolverRetryCount.class);
      RETRY_TIMEOUT = injector.getNamedInstance(NameResolverRetryTimeout.class);
    } catch (final InjectionException ex) {
      final String msg = "Exception while trying to find default values for retryCount & Timeout";
      LOG.log(Level.SEVERE, msg, ex);
      throw new RuntimeException(msg, ex);
    }
  }

  private final LocalAddressProvider localAddressProvider;
  @Rule
  public final TestName name = new TestName();
  static final long TTL = 30000;
  private final IdentifierFactory factory = new StringIdentifierFactory();
  private int port;

  public NamingTest() throws InjectionException {
    this.localAddressProvider = Tang.Factory.getTang().newInjector().getInstance(LocalAddressProvider.class);
  }

  /**
   * NameServer and NameLookupClient test.
   *
   * @throws Exception
   */
  @Test
  public void testNamingLookup() throws Exception {

    final String localAddress = localAddressProvider.getLocalAddress();
    LOG.log(Level.FINEST, this.name.getMethodName());

    // names 
    final Map<Identifier, InetSocketAddress> idToAddrMap = new HashMap<>();
    idToAddrMap.put(this.factory.getNewInstance("task1"), new InetSocketAddress(localAddress, 7001));
    idToAddrMap.put(this.factory.getNewInstance("task2"), new InetSocketAddress(localAddress, 7002));

    // run a server
    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileParameter(NameServerParameters.NameServerIdentifierFactory.class, this.factory);
    injector.bindVolatileInstance(LocalAddressProvider.class, this.localAddressProvider);
    try (final NameServer server = injector.getInstance(NameServer.class)) {
      this.port = server.getPort();
      for (final Identifier id : idToAddrMap.keySet()) {
        server.register(id, idToAddrMap.get(id));
      }

      // run a client
      try (final NameLookupClient client =
               getNewNameLookupClient(localAddress, port, TTL, RETRY_COUNT, RETRY_TIMEOUT,
                   Optional.of(this.localAddressProvider), Optional.of(this.factory))) {

        final Identifier id1 = this.factory.getNewInstance("task1");
        final Identifier id2 = this.factory.getNewInstance("task2");

        final Map<Identifier, InetSocketAddress> respMap = new HashMap<>();
        final InetSocketAddress addr1 = client.lookup(id1);
        respMap.put(id1, addr1);
        final InetSocketAddress addr2 = client.lookup(id2);
        respMap.put(id2, addr2);

        for (final Identifier id : respMap.keySet()) {
          LOG.log(Level.FINEST, "Mapping: {0} -> {1}", new Object[]{id, respMap.get(id)});
        }

        Assert.assertTrue(isEqual(idToAddrMap, respMap));
      }
    }
  }

  private static NameLookupClient getNewNameLookupClient(final String serverAddr,
                                                         final int serverPort,
                                                         final long timeout,
                                                         final int retryCount,
                                                         final int retryTimeout,
                                                         final Optional<LocalAddressProvider> localAddressProvider,
                                                         final Optional<IdentifierFactory> factory)
      throws InjectionException {


    final Configuration injectorConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(NameResolverNameServerAddr.class, serverAddr)
        .bindNamedParameter(NameResolverNameServerPort.class, Integer.toString(serverPort))
        .bindNamedParameter(NameResolverCacheTimeout.class, Long.toString(timeout))
        .bindNamedParameter(NameResolverRetryCount.class, Integer.toString(retryCount))
        .bindNamedParameter(NameResolverRetryTimeout.class, Integer.toString(retryTimeout))
        .build();

    final Injector injector = Tang.Factory.getTang().newInjector(injectorConf);
    if (localAddressProvider.isPresent()) {
      injector.bindVolatileInstance(LocalAddressProvider.class, localAddressProvider.get());
    }

    if (factory.isPresent()) {
      injector.bindVolatileInstance(IdentifierFactory.class, factory.get());
    }

    return injector.getInstance(NameLookupClient.class);
  }

  /**
   * Test concurrent lookups (threads share a client).
   *
   * @throws Exception
   */
  @Test
  public void testConcurrentNamingLookup() throws Exception {

    LOG.log(Level.FINEST, this.name.getMethodName());

    final String localAddress = localAddressProvider.getLocalAddress();
    // test it 3 times to make failure likely
    for (int i = 0; i < 3; i++) {

      LOG.log(Level.FINEST, "test {0}", i);

      // names 
      final Map<Identifier, InetSocketAddress> idToAddrMap = new HashMap<>();
      idToAddrMap.put(this.factory.getNewInstance("task1"), new InetSocketAddress(localAddress, 7001));
      idToAddrMap.put(this.factory.getNewInstance("task2"), new InetSocketAddress(localAddress, 7002));
      idToAddrMap.put(this.factory.getNewInstance("task3"), new InetSocketAddress(localAddress, 7003));

      // run a server
      final Injector injector = Tang.Factory.getTang().newInjector();
      injector.bindVolatileParameter(NameServerParameters.NameServerIdentifierFactory.class, this.factory);
      injector.bindVolatileInstance(LocalAddressProvider.class, this.localAddressProvider);
      try (final NameServer server = injector.getInstance(NameServer.class)) {
        this.port = server.getPort();
        for (final Identifier id : idToAddrMap.keySet()) {
          server.register(id, idToAddrMap.get(id));
        }

        // run a client
        try (final NameLookupClient client =
            getNewNameLookupClient(localAddress, port, TTL, RETRY_COUNT, RETRY_TIMEOUT,
                Optional.of(this.localAddressProvider), Optional.of(this.factory))) {
          final Identifier id1 = this.factory.getNewInstance("task1");
          final Identifier id2 = this.factory.getNewInstance("task2");
          final Identifier id3 = this.factory.getNewInstance("task3");

          final ExecutorService e = Executors.newCachedThreadPool();

          final ConcurrentMap<Identifier, InetSocketAddress> respMap = new ConcurrentHashMap<>();

          final Future<?> f1 = e.submit(new Runnable() {
            @Override
            public void run() {
              InetSocketAddress addr = null;
              try {
                addr = client.lookup(id1);
              } catch (final Exception e) {
                LOG.log(Level.SEVERE, "Lookup failed", e);
                Assert.fail(e.toString());
              }
              respMap.put(id1, addr);
            }
          });
          final Future<?> f2 = e.submit(new Runnable() {
            @Override
            public void run() {
              InetSocketAddress addr = null;
              try {
                addr = client.lookup(id2);
              } catch (final Exception e) {
                LOG.log(Level.SEVERE, "Lookup failed", e);
                Assert.fail(e.toString());
              }
              respMap.put(id2, addr);
            }
          });
          final Future<?> f3 = e.submit(new Runnable() {
            @Override
            public void run() {
              InetSocketAddress addr = null;
              try {
                addr = client.lookup(id3);
              } catch (final Exception e) {
                LOG.log(Level.SEVERE, "Lookup failed", e);
                Assert.fail(e.toString());
              }
              respMap.put(id3, addr);
            }
          });

          f1.get();
          f2.get();
          f3.get();

          for (final Identifier id : respMap.keySet()) {
            LOG.log(Level.FINEST, "Mapping: {0} -> {1}", new Object[]{id, respMap.get(id)});
          }

          Assert.assertTrue(isEqual(idToAddrMap, respMap));
        }
      }
    }
  }

  /**
   * NameServer and NameRegistryClient test.
   *
   * @throws Exception
   */
  @Test
  public void testNamingRegistry() throws Exception {

    LOG.log(Level.FINEST, this.name.getMethodName());

    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileParameter(NameServerParameters.NameServerIdentifierFactory.class, this.factory);
    injector.bindVolatileInstance(LocalAddressProvider.class, this.localAddressProvider);
    try (final NameServer server = injector.getInstance(NameServer.class)) {
      this.port = server.getPort();
      final String localAddress = localAddressProvider.getLocalAddress();

      // names to start with
      final Map<Identifier, InetSocketAddress> idToAddrMap = new HashMap<>();
      idToAddrMap.put(this.factory.getNewInstance("task1"), new InetSocketAddress(localAddress, 7001));
      idToAddrMap.put(this.factory.getNewInstance("task2"), new InetSocketAddress(localAddress, 7002));

      // registration
      // invoke registration from the client side
      try (final NameRegistryClient client = 
          new NameRegistryClient(localAddress, this.port, this.factory, this.localAddressProvider)) {
        for (final Identifier id : idToAddrMap.keySet()) {
          client.register(id, idToAddrMap.get(id));
        }

        // wait
        final Set<Identifier> ids = idToAddrMap.keySet();
        busyWait(server, ids.size(), ids);

        // check the server side
        Map<Identifier, InetSocketAddress> serverMap = new HashMap<>();
        Iterable<NameAssignment> nas = server.lookup(ids);

        for (final NameAssignment na : nas) {
          LOG.log(Level.FINEST, "Mapping: {0} -> {1}",
              new Object[]{na.getIdentifier(), na.getAddress()});
          serverMap.put(na.getIdentifier(), na.getAddress());
        }

        Assert.assertTrue(isEqual(idToAddrMap, serverMap));

        // un-registration
        for (final Identifier id : idToAddrMap.keySet()) {
          client.unregister(id);
        }

        // wait
        busyWait(server, 0, ids);

        serverMap = new HashMap<>();
        nas = server.lookup(ids);
        for (final NameAssignment na : nas) {
          serverMap.put(na.getIdentifier(), na.getAddress());
        }

        Assert.assertEquals(0, serverMap.size());
      }
    }
  }

  /**
   * NameServer and NameClient test.
   *
   * @throws Exception
   */
  @Test
  public void testNameClient() throws Exception {

    LOG.log(Level.FINEST, this.name.getMethodName());

    final String localAddress = localAddressProvider.getLocalAddress();
    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileParameter(NameServerParameters.NameServerIdentifierFactory.class, this.factory);
    injector.bindVolatileInstance(LocalAddressProvider.class, this.localAddressProvider);
    try (final NameServer server = injector.getInstance(NameServer.class)) {
      this.port = server.getPort();

      final Map<Identifier, InetSocketAddress> idToAddrMap = new HashMap<>();
      idToAddrMap.put(this.factory.getNewInstance("task1"), new InetSocketAddress(localAddress, 7001));
      idToAddrMap.put(this.factory.getNewInstance("task2"), new InetSocketAddress(localAddress, 7002));

      // registration
      // invoke registration from the client side
      final Configuration nameResolverConf = NameResolverConfiguration.CONF
          .set(NameResolverConfiguration.NAME_SERVER_HOSTNAME, localAddress)
          .set(NameResolverConfiguration.NAME_SERVICE_PORT, this.port)
          .set(NameResolverConfiguration.CACHE_TIMEOUT, TTL)
          .set(NameResolverConfiguration.RETRY_TIMEOUT, RETRY_TIMEOUT)
          .set(NameResolverConfiguration.RETRY_COUNT, RETRY_COUNT)
          .build();

      try (final NameResolver client
               = Tang.Factory.getTang().newInjector(nameResolverConf).getInstance(NameClient.class)) {
        for (final Identifier id : idToAddrMap.keySet()) {
          client.register(id, idToAddrMap.get(id));
        }

        // wait
        final Set<Identifier> ids = idToAddrMap.keySet();
        busyWait(server, ids.size(), ids);

        // lookup
        final Identifier id1 = this.factory.getNewInstance("task1");
        final Identifier id2 = this.factory.getNewInstance("task2");

        final Map<Identifier, InetSocketAddress> respMap = new HashMap<>();
        InetSocketAddress addr1 = client.lookup(id1);
        respMap.put(id1, addr1);
        InetSocketAddress addr2 = client.lookup(id2);
        respMap.put(id2, addr2);

        for (final Identifier id : respMap.keySet()) {
          LOG.log(Level.FINEST, "Mapping: {0} -> {1}", new Object[]{id, respMap.get(id)});
        }

        Assert.assertTrue(isEqual(idToAddrMap, respMap));

        // un-registration
        for (final Identifier id : idToAddrMap.keySet()) {
          client.unregister(id);
        }

        // wait
        busyWait(server, 0, ids);

        final Map<Identifier, InetSocketAddress> serverMap = new HashMap<>();
        addr1 = server.lookup(id1);
        if (addr1 != null) {
          serverMap.put(id1, addr1);
        }
        addr2 = server.lookup(id1);
        if (addr2 != null) {
          serverMap.put(id2, addr2);
        }

        Assert.assertEquals(0, serverMap.size());
      }
    }
  }

  private boolean isEqual(final Map<Identifier, InetSocketAddress> map1,
                          final Map<Identifier, InetSocketAddress> map2) {

    if (map1.size() != map2.size()) {
      return false;
    }

    for (final Identifier id : map1.keySet()) {
      final InetSocketAddress addr1 = map1.get(id);
      final InetSocketAddress addr2 = map2.get(id);
      if (!addr1.equals(addr2)) {
        return false;
      }
    }

    return true;
  }

  private void busyWait(final NameServer server, final int expected, final Set<Identifier> ids) {
    int count = 0;
    for (;;) {
      final Iterable<NameAssignment> nas = server.lookup(ids);
      for (@SuppressWarnings("unused") final NameAssignment na : nas) {
        ++count;
      }
      if (count == expected) {
        break;
      }
      count = 0;
    }
  }
}
