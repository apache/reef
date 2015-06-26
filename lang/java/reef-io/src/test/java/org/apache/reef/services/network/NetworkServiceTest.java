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
package org.apache.reef.services.network;

import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.Connection;
import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.impl.NetworkService;
import org.apache.reef.io.network.naming.*;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.services.network.util.Monitor;
import org.apache.reef.services.network.util.StringCodec;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.remote.address.LocalAddressProviderFactory;
import org.apache.reef.wake.remote.transport.netty.MessagingTransportFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.net.InetSocketAddress;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Network service test
 */
public class NetworkServiceTest {
  private static final Logger LOG = Logger.getLogger(NetworkServiceTest.class.getName());

  private final LocalAddressProvider localAddressProvider;
  private final String localAddress;

  public NetworkServiceTest() throws InjectionException {
    localAddressProvider = LocalAddressProviderFactory.getInstance();
    localAddress = localAddressProvider.getLocalAddress();
  }

  @Rule
  public TestName name = new TestName();

  /**
   * NetworkService messaging test
   */
  @Test
  public void testMessagingNetworkService() throws Exception {
    LOG.log(Level.FINEST, name.getMethodName());

    IdentifierFactory factory = new StringIdentifierFactory();

    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileParameter(NameServerParameters.NameServerIdentifierFactory.class, factory);
    injector.bindVolatileInstance(LocalAddressProvider.class, this.localAddressProvider);
    final NameServer server = injector.getInstance(NameServer.class);
    int nameServerPort = server.getPort();

    final int numMessages = 10;
    final Monitor monitor = new Monitor();

    LOG.log(Level.FINEST, "=== Test network service receiver start");
    // network service
    final String name2 = "task2";
    final Configuration nameResolverConf = Tang.Factory.getTang().newConfigurationBuilder(NameResolverConfiguration.CONF
        .set(NameResolverConfiguration.NAME_SERVER_HOSTNAME, this.localAddress)
        .set(NameResolverConfiguration.NAME_SERVICE_PORT, nameServerPort)
        .build())
        .build();

    final Injector injector2 = Tang.Factory.getTang().newInjector(nameResolverConf);
    final NameResolver nameResolver = injector2.getInstance(NameResolver.class);

    NetworkService<String> ns2 = new NetworkService<String>(
        factory, 0, nameResolver,
        new StringCodec(), new MessagingTransportFactory(localAddressProvider),
        new MessageHandler<String>(name2, monitor, numMessages), new ExceptionHandler(), localAddressProvider);
    ns2.registerId(factory.getNewInstance(name2));
    final int port2 = ns2.getTransport().getListeningPort();
    server.register(factory.getNewInstance("task2"), new InetSocketAddress(this.localAddress, port2));

    LOG.log(Level.FINEST, "=== Test network service sender start");
    final String name1 = "task1";
    final NetworkService<String> ns1 = new NetworkService<String>(factory, 0, nameResolver,
        new StringCodec(), new MessagingTransportFactory(localAddressProvider),
        new MessageHandler<String>(name1, null, 0), new ExceptionHandler(), localAddressProvider);
    ns1.registerId(factory.getNewInstance(name1));
    final int port1 = ns1.getTransport().getListeningPort();
    server.register(factory.getNewInstance("task1"), new InetSocketAddress(this.localAddress, port1));

    final Identifier destId = factory.getNewInstance(name2);
    final Connection<String> conn = ns1.newConnection(destId);
    try {
      conn.open();
      for (int count = 0; count < numMessages; ++count) {
        conn.write("hello! " + count);
      }
      monitor.mwait();

    } catch (NetworkException e) {
      e.printStackTrace();
    }
    conn.close();

    ns1.close();
    ns2.close();

    server.close();
  }

  /**
   * NetworkService messaging rate benchmark
   */
  @Test
  public void testMessagingNetworkServiceRate() throws Exception {
    LOG.log(Level.FINEST, name.getMethodName());

    IdentifierFactory factory = new StringIdentifierFactory();

    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileParameter(NameServerParameters.NameServerIdentifierFactory.class, factory);
    injector.bindVolatileInstance(LocalAddressProvider.class, this.localAddressProvider);
    final NameServer server = injector.getInstance(NameServer.class);
    int nameServerPort = server.getPort();

    final int[] messageSizes = {1, 16, 32, 64, 512, 64 * 1024, 1024 * 1024};

    for (int size : messageSizes) {
      final int numMessages = 300000 / (Math.max(1, size / 512));
      final Monitor monitor = new Monitor();

      LOG.log(Level.FINEST, "=== Test network service receiver start");
      // network service
      final String name2 = "task2";
      final Configuration nameResolverConf = Tang.Factory.getTang().newConfigurationBuilder(NameResolverConfiguration.CONF
          .set(NameResolverConfiguration.NAME_SERVER_HOSTNAME, this.localAddress)
          .set(NameResolverConfiguration.NAME_SERVICE_PORT, nameServerPort)
          .build())
          .build();

      final Injector injector2 = Tang.Factory.getTang().newInjector(nameResolverConf);
      final NameResolver nameResolver = injector2.getInstance(NameResolver.class);

      NetworkService<String> ns2 = new NetworkService<String>(
          factory, 0, nameResolver,
          new StringCodec(), new MessagingTransportFactory(localAddressProvider),
          new MessageHandler<String>(name2, monitor, numMessages), new ExceptionHandler(), localAddressProvider);
      ns2.registerId(factory.getNewInstance(name2));
      final int port2 = ns2.getTransport().getListeningPort();
      server.register(factory.getNewInstance("task2"), new InetSocketAddress(this.localAddress, port2));

      LOG.log(Level.FINEST, "=== Test network service sender start");
      final String name1 = "task1";
      NetworkService<String> ns1 = new NetworkService<String>(
          factory, 0, nameResolver,
          new StringCodec(), new MessagingTransportFactory(localAddressProvider),
          new MessageHandler<String>(name1, null, 0), new ExceptionHandler(), localAddressProvider);
      ns1.registerId(factory.getNewInstance(name1));
      final int port1 = ns1.getTransport().getListeningPort();
      server.register(factory.getNewInstance("task1"), new InetSocketAddress(this.localAddress, port1));

      Identifier destId = factory.getNewInstance(name2);
      Connection<String> conn = ns1.newConnection(destId);

      // build the message
      StringBuilder msb = new StringBuilder();
      for (int i = 0; i < size; i++) {
        msb.append("1");
      }
      String message = msb.toString();

      long start = System.currentTimeMillis();
      try {
        for (int i = 0; i < numMessages; i++) {
          conn.open();
          conn.write(message);
        }
        monitor.mwait();
      } catch (NetworkException e) {
        e.printStackTrace();
      }
      long end = System.currentTimeMillis();
      double runtime = ((double) end - start) / 1000;
      LOG.log(Level.FINEST, "size: " + size + "; messages/s: " + numMessages / runtime + " bandwidth(bytes/s): " + ((double) numMessages * 2 * size) / runtime);// x2 for unicode chars
      conn.close();

      ns1.close();
      ns2.close();
    }

    server.close();
  }

  /**
   * NetworkService messaging rate benchmark
   */
  @Test
  public void testMessagingNetworkServiceRateDisjoint() throws Exception {
    LOG.log(Level.FINEST, name.getMethodName());

    final IdentifierFactory factory = new StringIdentifierFactory();

    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileParameter(NameServerParameters.NameServerIdentifierFactory.class, factory);
    injector.bindVolatileInstance(LocalAddressProvider.class, this.localAddressProvider);
    final NameServer server = injector.getInstance(NameServer.class);
    final int nameServerPort = server.getPort();

    BlockingQueue<Object> barrier = new LinkedBlockingQueue<Object>();

    int numThreads = 4;
    final int size = 2000;
    final int numMessages = 300000 / (Math.max(1, size / 512));
    final int totalNumMessages = numMessages * numThreads;

    ExecutorService e = Executors.newCachedThreadPool();
    for (int t = 0; t < numThreads; t++) {
      final int tt = t;

      e.submit(new Runnable() {
        public void run() {
          try {
            Monitor monitor = new Monitor();

            LOG.log(Level.FINEST, "=== Test network service receiver start");
            // network service
            final String name2 = "task2-" + tt;
            final Configuration nameResolverConf = Tang.Factory.getTang().newConfigurationBuilder(NameResolverConfiguration.CONF
                .set(NameResolverConfiguration.NAME_SERVER_HOSTNAME, localAddress)
                .set(NameResolverConfiguration.NAME_SERVICE_PORT, nameServerPort)
                .build())
                .build();

            final Injector injector = Tang.Factory.getTang().newInjector(nameResolverConf);
            final NameResolver nameResolver = injector.getInstance(NameResolver.class);
            NetworkService<String> ns2 = new NetworkService<String>(
                factory, 0, nameResolver,
                new StringCodec(), new MessagingTransportFactory(localAddressProvider),
                new MessageHandler<String>(name2, monitor, numMessages), new ExceptionHandler(), localAddressProvider);
            ns2.registerId(factory.getNewInstance(name2));
            final int port2 = ns2.getTransport().getListeningPort();
            server.register(factory.getNewInstance(name2), new InetSocketAddress(localAddress, port2));

            LOG.log(Level.FINEST, "=== Test network service sender start");
            final String name1 = "task1-" + tt;
            NetworkService<String> ns1 = new NetworkService<String>(
                factory, 0, nameResolver,
                new StringCodec(), new MessagingTransportFactory(localAddressProvider),
                new MessageHandler<String>(name1, null, 0), new ExceptionHandler(), localAddressProvider);
            ns1.registerId(factory.getNewInstance(name1));
            final int port1 = ns1.getTransport().getListeningPort();
            server.register(factory.getNewInstance(name1), new InetSocketAddress(localAddress, port1));

            Identifier destId = factory.getNewInstance(name2);
            Connection<String> conn = ns1.newConnection(destId);

            // build the message
            StringBuilder msb = new StringBuilder();
            for (int i = 0; i < size; i++) {
              msb.append("1");
            }
            String message = msb.toString();


            try {
              for (int i = 0; i < numMessages; i++) {
                conn.open();
                conn.write(message);
              }
              monitor.mwait();
            } catch (NetworkException e) {
              e.printStackTrace();
            }
            conn.close();

            ns1.close();
            ns2.close();
          } catch (Exception e) {
            e.printStackTrace();

          }
        }
      });
    }

    // start and time
    long start = System.currentTimeMillis();
    Object ignore = new Object();
    for (int i = 0; i < numThreads; i++) barrier.add(ignore);
    e.shutdown();
    e.awaitTermination(100, TimeUnit.SECONDS);
    long end = System.currentTimeMillis();

    double runtime = ((double) end - start) / 1000;
    LOG.log(Level.FINEST, "size: " + size + "; messages/s: " + totalNumMessages / runtime + " bandwidth(bytes/s): " + ((double) totalNumMessages * 2 * size) / runtime);// x2 for unicode chars

    server.close();
  }

  @Test
  public void testMultithreadedSharedConnMessagingNetworkServiceRate() throws Exception {
    LOG.log(Level.FINEST, name.getMethodName());

    IdentifierFactory factory = new StringIdentifierFactory();

    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileParameter(NameServerParameters.NameServerIdentifierFactory.class, factory);
    injector.bindVolatileInstance(LocalAddressProvider.class, this.localAddressProvider);
    final NameServer server = injector.getInstance(NameServer.class);
    int nameServerPort = server.getPort();

    final int[] messageSizes = {2000};// {1,16,32,64,512,64*1024,1024*1024};

    for (int size : messageSizes) {
      final int numMessages = 300000 / (Math.max(1, size / 512));
      int numThreads = 2;
      int totalNumMessages = numMessages * numThreads;
      final Monitor monitor = new Monitor();

      LOG.log(Level.FINEST, "=== Test network service receiver start");
      // network service
      final String name2 = "task2";
      final Configuration nameResolverConf = Tang.Factory.getTang().newConfigurationBuilder(NameResolverConfiguration.CONF
          .set(NameResolverConfiguration.NAME_SERVER_HOSTNAME, this.localAddress)
          .set(NameResolverConfiguration.NAME_SERVICE_PORT, nameServerPort)
          .build())
          .build();

      final Injector injector2 = Tang.Factory.getTang().newInjector(nameResolverConf);
      final NameResolver nameResolver = injector2.getInstance(NameResolver.class);
      NetworkService<String> ns2 = new NetworkService<String>(
          factory, 0, nameResolver,
          new StringCodec(), new MessagingTransportFactory(localAddressProvider),
          new MessageHandler<String>(name2, monitor, totalNumMessages), new ExceptionHandler(), localAddressProvider);
      ns2.registerId(factory.getNewInstance(name2));
      final int port2 = ns2.getTransport().getListeningPort();
      server.register(factory.getNewInstance("task2"), new InetSocketAddress(this.localAddress, port2));

      LOG.log(Level.FINEST, "=== Test network service sender start");
      final String name1 = "task1";
      NetworkService<String> ns1 = new NetworkService<String>(
          factory, 0, nameResolver,
          new StringCodec(), new MessagingTransportFactory(localAddressProvider),
          new MessageHandler<String>(name1, null, 0), new ExceptionHandler(), localAddressProvider);
      ns1.registerId(factory.getNewInstance(name1));
      final int port1 = ns1.getTransport().getListeningPort();
      server.register(factory.getNewInstance("task1"), new InetSocketAddress(this.localAddress, port1));

      Identifier destId = factory.getNewInstance(name2);
      final Connection<String> conn = ns1.newConnection(destId);
      conn.open();

      // build the message
      StringBuilder msb = new StringBuilder();
      for (int i = 0; i < size; i++) {
        msb.append("1");
      }
      final String message = msb.toString();

      ExecutorService e = Executors.newCachedThreadPool();

      long start = System.currentTimeMillis();
      for (int i = 0; i < numThreads; i++) {
        e.submit(new Runnable() {

          @Override
          public void run() {
            try {
              for (int i = 0; i < numMessages; i++) {
                conn.write(message);
              }
            } catch (NetworkException e) {
              e.printStackTrace();
            }
          }
        });
      }


      e.shutdown();
      e.awaitTermination(30, TimeUnit.SECONDS);
      monitor.mwait();

      long end = System.currentTimeMillis();
      double runtime = ((double) end - start) / 1000;

      LOG.log(Level.FINEST, "size: " + size + "; messages/s: " + totalNumMessages / runtime + " bandwidth(bytes/s): " + ((double) totalNumMessages * 2 * size) / runtime);// x2 for unicode chars
      conn.close();

      ns1.close();
      ns2.close();
    }

    server.close();
  }

  /**
   * NetworkService messaging rate benchmark
   */
  @Test
  public void testMessagingNetworkServiceBatchingRate() throws Exception {
    LOG.log(Level.FINEST, name.getMethodName());

    IdentifierFactory factory = new StringIdentifierFactory();

    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileParameter(NameServerParameters.NameServerIdentifierFactory.class, factory);
    injector.bindVolatileInstance(LocalAddressProvider.class, this.localAddressProvider);
    final NameServer server = injector.getInstance(NameServer.class);
    int nameServerPort = server.getPort();

    final int batchSize = 1024 * 1024;
    final int[] messageSizes = {32, 64, 512};

    for (int size : messageSizes) {
      final int numMessages = 300 / (Math.max(1, size / 512));
      final Monitor monitor = new Monitor();

      LOG.log(Level.FINEST, "=== Test network service receiver start");
      // network service
      final String name2 = "task2";
      final Configuration nameResolverConf = Tang.Factory.getTang().newConfigurationBuilder(NameResolverConfiguration.CONF
          .set(NameResolverConfiguration.NAME_SERVER_HOSTNAME, this.localAddress)
          .set(NameResolverConfiguration.NAME_SERVICE_PORT, nameServerPort)
          .build())
          .build();

      final Injector injector2 = Tang.Factory.getTang().newInjector(nameResolverConf);
      final NameResolver nameResolver = injector2.getInstance(NameResolver.class);
      NetworkService<String> ns2 = new NetworkService<String>(
          factory, 0, nameResolver,
          new StringCodec(), new MessagingTransportFactory(localAddressProvider),
          new MessageHandler<String>(name2, monitor, numMessages), new ExceptionHandler(), localAddressProvider);
      ns2.registerId(factory.getNewInstance(name2));
      final int port2 = ns2.getTransport().getListeningPort();
      server.register(factory.getNewInstance("task2"), new InetSocketAddress(this.localAddress, port2));

      LOG.log(Level.FINEST, "=== Test network service sender start");
      final String name1 = "task1";
      NetworkService<String> ns1 = new NetworkService<String>(
          factory, 0, nameResolver,
          new StringCodec(), new MessagingTransportFactory(localAddressProvider),
          new MessageHandler<String>(name1, null, 0), new ExceptionHandler(), localAddressProvider);
      ns1.registerId(factory.getNewInstance(name1));
      final int port1 = ns1.getTransport().getListeningPort();
      server.register(factory.getNewInstance("task1"), new InetSocketAddress(this.localAddress, port1));

      Identifier destId = factory.getNewInstance(name2);
      Connection<String> conn = ns1.newConnection(destId);

      // build the message
      StringBuilder msb = new StringBuilder();
      for (int i = 0; i < size; i++) {
        msb.append("1");
      }
      String message = msb.toString();

      long start = System.currentTimeMillis();
      try {
        for (int i = 0; i < numMessages; i++) {
          StringBuilder sb = new StringBuilder();
          for (int j = 0; j < batchSize / size; j++) {
            sb.append(message);
          }
          conn.open();
          conn.write(sb.toString());
        }
        monitor.mwait();
      } catch (NetworkException e) {
        e.printStackTrace();
      }
      long end = System.currentTimeMillis();
      double runtime = ((double) end - start) / 1000;
      long numAppMessages = numMessages * batchSize / size;
      LOG.log(Level.FINEST, "size: " + size + "; messages/s: " + numAppMessages / runtime + " bandwidth(bytes/s): " + ((double) numAppMessages * 2 * size) / runtime);// x2 for unicode chars
      conn.close();

      ns1.close();
      ns2.close();
    }

    server.close();
  }

  /**
   * Test message handler
   */
  class MessageHandler<T> implements EventHandler<Message<T>> {

    private final String name;
    private final int expected;
    private final Monitor monitor;
    private AtomicInteger count = new AtomicInteger(0);

    MessageHandler(String name, Monitor monitor, int expected) {
      this.name = name;
      this.monitor = monitor;
      this.expected = expected;
    }

    @Override
    public void onNext(Message<T> value) {

      count.incrementAndGet();

      LOG.log(Level.FINEST,
          "OUT: {0} received {1} from {2} to {3}",
          new Object[]{name, value.getData(), value.getSrcId(), value.getDestId()});

      for (final T obj : value.getData()) {
        LOG.log(Level.FINEST, "OUT: data: {0}", obj);
      }

      if (count.get() == expected) {
        monitor.mnotify();
      }
    }
  }

  /**
   * Test exception handler
   */
  class ExceptionHandler implements EventHandler<Exception> {
    @Override
    public void onNext(Exception error) {
      System.err.println(error);
    }
  }
}
