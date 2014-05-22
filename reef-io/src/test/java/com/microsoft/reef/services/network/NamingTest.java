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

import com.microsoft.reef.io.naming.NameAssignment;
import com.microsoft.reef.io.network.naming.*;
import com.microsoft.reef.io.network.util.StringIdentifierFactory;
import com.microsoft.tang.Injector;
import com.microsoft.tang.Tang;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.wake.Identifier;
import com.microsoft.wake.IdentifierFactory;
import com.microsoft.wake.remote.NetUtils;
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
 * Naming server and client test
 */
public class NamingTest {

  private static final Logger LOG = Logger.getLogger(NamingTest.class.getName());

  @Rule
  public final TestName name = new TestName();

  final long TTL = 30000;
  int port;
  final IdentifierFactory factory = new StringIdentifierFactory();

  private static final int retryCount;
  private static final int retryTimeout;

  static {
    try {
      final Injector injector = Tang.Factory.getTang().newInjector();
      retryCount = injector.getNamedInstance(NameLookupClient.RetryCount.class);
      retryTimeout = injector.getNamedInstance(NameLookupClient.RetryTimeout.class);
    } catch (final InjectionException ex) {
      final String msg = "Exception while trying to find default values for retryCount & Timeout";
      LOG.log(Level.SEVERE, msg, ex);
      throw new RuntimeException(msg, ex);
    }
  }

  /**
   * NameServer and NameLookupClient test
   *
   * @throws Exception
   */
  @Test
  public void testNamingLookup() throws Exception {

    LOG.log(Level.FINEST, this.name.getMethodName());

    // names 
    final Map<Identifier, InetSocketAddress> idToAddrMap = new HashMap<Identifier, InetSocketAddress>();
    idToAddrMap.put(this.factory.getNewInstance("task1"), new InetSocketAddress(NetUtils.getLocalAddress(), 7001));
    idToAddrMap.put(this.factory.getNewInstance("task2"), new InetSocketAddress(NetUtils.getLocalAddress(), 7002));

    // run a server
    final NameServer server = new NameServer(0, this.factory);
    this.port = server.getPort();
    for (final Identifier id : idToAddrMap.keySet()) {
      server.register(id, idToAddrMap.get(id));
    }

    // run a client
    final NameLookupClient client = new NameLookupClient(NetUtils.getLocalAddress(), this.port,
        10000, this.factory, retryCount, retryTimeout, new NameCache(this.TTL));

    final Identifier id1 = this.factory.getNewInstance("task1");
    final Identifier id2 = this.factory.getNewInstance("task2");

    final Map<Identifier, InetSocketAddress> respMap = new HashMap<Identifier, InetSocketAddress>();
    InetSocketAddress addr1 = client.lookup(id1);
    respMap.put(id1, addr1);
    InetSocketAddress addr2 = client.lookup(id2);
    respMap.put(id2, addr2);

    for (final Identifier id : respMap.keySet()) {
      LOG.log(Level.FINEST, "Mapping: {0} -> {1}", new Object[] { id, respMap.get(id) });
    }

    Assert.assertTrue(isEqual(idToAddrMap, respMap));

    client.close();
    server.close();
  }

  /**
   * Test concurrent lookups (threads share a client)
   *
   * @throws Exception
   */
  @Test
  public void testConcurrentNamingLookup() throws Exception {

    LOG.log(Level.FINEST, this.name.getMethodName());

    // test it 3 times to make failure likely
    for (int i = 0; i < 3; i++) {

      LOG.log(Level.FINEST, "test {0}", i);

      // names 
      final Map<Identifier, InetSocketAddress> idToAddrMap = new HashMap<Identifier, InetSocketAddress>();
      idToAddrMap.put(this.factory.getNewInstance("task1"), new InetSocketAddress(NetUtils.getLocalAddress(), 7001));
      idToAddrMap.put(this.factory.getNewInstance("task2"), new InetSocketAddress(NetUtils.getLocalAddress(), 7002));
      idToAddrMap.put(this.factory.getNewInstance("task3"), new InetSocketAddress(NetUtils.getLocalAddress(), 7003));

      // run a server
      final NameServer server = new NameServer(0, this.factory);
      this.port = server.getPort();
      for (final Identifier id : idToAddrMap.keySet()) {
        server.register(id, idToAddrMap.get(id));
      }

      // run a client
      final NameLookupClient client = new NameLookupClient(NetUtils.getLocalAddress(), this.port,
          10000, this.factory, retryCount, retryTimeout, new NameCache(this.TTL));

      final Identifier id1 = this.factory.getNewInstance("task1");
      final Identifier id2 = this.factory.getNewInstance("task2");
      final Identifier id3 = this.factory.getNewInstance("task3");

      final ExecutorService e = Executors.newCachedThreadPool();

      final ConcurrentMap<Identifier, InetSocketAddress> respMap = new ConcurrentHashMap<Identifier, InetSocketAddress>();

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
        LOG.log(Level.FINEST, "Mapping: {0} -> {1}", new Object[] { id, respMap.get(id) });
      }

      Assert.assertTrue(isEqual(idToAddrMap, respMap));

      client.close();
      server.close();
    }
  }

  /**
   * NameServer and NameRegistryClient test
   *
   * @throws Exception
   */
  @Test
  public void testNamingRegistry() throws Exception {

    LOG.log(Level.FINEST, this.name.getMethodName());

    final NameServer server = new NameServer(0, this.factory);
    this.port = server.getPort();

    // names to start with
    final Map<Identifier, InetSocketAddress> idToAddrMap = new HashMap<Identifier, InetSocketAddress>();
    idToAddrMap.put(this.factory.getNewInstance("task1"), new InetSocketAddress(NetUtils.getLocalAddress(), 7001));
    idToAddrMap.put(this.factory.getNewInstance("task2"), new InetSocketAddress(NetUtils.getLocalAddress(), 7002));

    // registration
    // invoke registration from the client side
    final NameRegistryClient client = new NameRegistryClient(
        NetUtils.getLocalAddress(), this.port, this.factory);
    for (final Identifier id : idToAddrMap.keySet()) {
      client.register(id, idToAddrMap.get(id));
    }

    // wait
    final Set<Identifier> ids = idToAddrMap.keySet();
    busyWait(server, ids.size(), ids);

    // check the server side 
    Map<Identifier, InetSocketAddress> serverMap = new HashMap<Identifier, InetSocketAddress>();
    Iterable<NameAssignment> nas = server.lookup(ids);

    for (final NameAssignment na : nas) {
      LOG.log(Level.FINEST, "Mapping: {0} -> {1}",
          new Object[] { na.getIdentifier(), na.getAddress() });
      serverMap.put(na.getIdentifier(), na.getAddress());
    }

    Assert.assertTrue(isEqual(idToAddrMap, serverMap));

    // un-registration
    for (final Identifier id : idToAddrMap.keySet()) {
      client.unregister(id);
    }

    // wait
    busyWait(server, 0, ids);

    serverMap = new HashMap<Identifier, InetSocketAddress>();
    nas = server.lookup(ids);
    for (final NameAssignment na : nas)
      serverMap.put(na.getIdentifier(), na.getAddress());

    Assert.assertEquals(0, serverMap.size());

    client.close();
    server.close();
  }

  /**
   * NameServer and NameClient test
   *
   * @throws Exception
   */
  @Test
  public void testNameClient() throws Exception {

    LOG.log(Level.FINEST, this.name.getMethodName());

    final NameServer server = new NameServer(0, this.factory);
    this.port = server.getPort();

    final Map<Identifier, InetSocketAddress> idToAddrMap = new HashMap<Identifier, InetSocketAddress>();
    idToAddrMap.put(this.factory.getNewInstance("task1"), new InetSocketAddress(NetUtils.getLocalAddress(), 7001));
    idToAddrMap.put(this.factory.getNewInstance("task2"), new InetSocketAddress(NetUtils.getLocalAddress(), 7002));

    // registration
    // invoke registration from the client side
    final NameClient client = new NameClient(NetUtils.getLocalAddress(), this.port,
        this.factory, retryCount, retryTimeout, new NameCache(this.TTL));
    for (final Identifier id : idToAddrMap.keySet()) {
      client.register(id, idToAddrMap.get(id));
    }

    // wait
    final Set<Identifier> ids = idToAddrMap.keySet();
    busyWait(server, ids.size(), ids);

    // lookup
    final Identifier id1 = this.factory.getNewInstance("task1");
    final Identifier id2 = this.factory.getNewInstance("task2");

    final Map<Identifier, InetSocketAddress> respMap = new HashMap<Identifier, InetSocketAddress>();
    InetSocketAddress addr1 = client.lookup(id1);
    respMap.put(id1, addr1);
    InetSocketAddress addr2 = client.lookup(id2);
    respMap.put(id2, addr2);

    for (final Identifier id : respMap.keySet()) {
      LOG.log(Level.FINEST, "Mapping: {0} -> {1}", new Object[] { id, respMap.get(id) });
    }

    Assert.assertTrue(isEqual(idToAddrMap, respMap));

    // un-registration
    for (final Identifier id : idToAddrMap.keySet()) {
      client.unregister(id);
    }

    // wait
    busyWait(server, 0, ids);

    final Map<Identifier, InetSocketAddress> serverMap = new HashMap<Identifier, InetSocketAddress>();
    addr1 = server.lookup(id1);
    if (addr1 != null) serverMap.put(id1, addr1);
    addr2 = server.lookup(id1);
    if (addr2 != null) serverMap.put(id2, addr2);

    Assert.assertEquals(0, serverMap.size());

    client.close();
    server.close();
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
      for (final @SuppressWarnings("unused") NameAssignment na : nas) {
        ++count;
      }
      if (count == expected) {
        break;
      }
      count = 0;
    }
  }
}
