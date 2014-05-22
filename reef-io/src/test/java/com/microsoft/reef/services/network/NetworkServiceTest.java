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

import com.microsoft.reef.exception.evaluator.NetworkException;
import com.microsoft.reef.io.network.Connection;
import com.microsoft.reef.io.network.Message;
import com.microsoft.reef.io.network.impl.MessagingTransportFactory;
import com.microsoft.reef.io.network.impl.NetworkService;
import com.microsoft.reef.io.network.naming.NameServer;
import com.microsoft.reef.io.network.util.StringIdentifierFactory;
import com.microsoft.reef.services.network.util.Monitor;
import com.microsoft.reef.services.network.util.StringCodec;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.Identifier;
import com.microsoft.wake.IdentifierFactory;
import com.microsoft.wake.remote.NetUtils;
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

  @Rule
  public TestName name = new TestName();

  /**
   * NetworkService messaging test
   *
   * @throws Exception
   */
  @Test
  public void testMessagingNetworkService() throws Exception {
    LOG.log(Level.FINEST, name.getMethodName());

    IdentifierFactory factory = new StringIdentifierFactory();
    String nameServerAddr = NetUtils.getLocalAddress();

    NameServer server = new NameServer(0, factory);
    int nameServerPort = server.getPort();

    final int numMessages = 10;
    final Monitor monitor = new Monitor();

    LOG.log(Level.FINEST, "=== Test network service receiver start");
    // network service
    final String name2 = "task2";
    NetworkService<String> ns2 = new NetworkService<String>(
        factory, 0, nameServerAddr, nameServerPort,
        new StringCodec(), new MessagingTransportFactory(),
        new MessageHandler<String>(name2, monitor, numMessages), new ExceptionHandler());
    ns2.registerId(factory.getNewInstance(name2));
    final int port2 = ns2.getTransport().getListeningPort();
    server.register(factory.getNewInstance("task2"), new InetSocketAddress(nameServerAddr, port2));

    LOG.log(Level.FINEST, "=== Test network service sender start");
    final String name1 = "task1";
    final NetworkService<String> ns1 = new NetworkService<String>(factory, 0, nameServerAddr, nameServerPort,
        new StringCodec(), new MessagingTransportFactory(),
        new MessageHandler<String>(name1, null, 0), new ExceptionHandler());
    ns1.registerId(factory.getNewInstance(name1));
    final int port1 = ns1.getTransport().getListeningPort();
    server.register(factory.getNewInstance("task1"), new InetSocketAddress(nameServerAddr, port1));

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
   *
   * @throws Exception
   */
  @Test
  public void testMessagingNetworkServiceRate() throws Exception {
    LOG.log(Level.FINEST, name.getMethodName());

    IdentifierFactory factory = new StringIdentifierFactory();
    String nameServerAddr = NetUtils.getLocalAddress();

    NameServer server = new NameServer(0, factory);
    int nameServerPort = server.getPort();

    final int[] messageSizes = {1, 16, 32, 64, 512, 64 * 1024, 1024 * 1024};

    for (int size : messageSizes) {
      final int numMessages = 300000 / (Math.max(1, size / 512));
      final Monitor monitor = new Monitor();

      LOG.log(Level.FINEST, "=== Test network service receiver start");
      // network service
      final String name2 = "task2";
      NetworkService<String> ns2 = new NetworkService<String>(
          factory, 0, nameServerAddr, nameServerPort,
          new StringCodec(), new MessagingTransportFactory(),
          new MessageHandler<String>(name2, monitor, numMessages), new ExceptionHandler());
      ns2.registerId(factory.getNewInstance(name2));
      final int port2 = ns2.getTransport().getListeningPort();
      server.register(factory.getNewInstance("task2"), new InetSocketAddress(nameServerAddr, port2));

      LOG.log(Level.FINEST, "=== Test network service sender start");
      final String name1 = "task1";
      NetworkService<String> ns1 = new NetworkService<String>(
          factory, 0, nameServerAddr, nameServerPort,
          new StringCodec(), new MessagingTransportFactory(),
          new MessageHandler<String>(name1, null, 0), new ExceptionHandler());
      ns1.registerId(factory.getNewInstance(name1));
      final int port1 = ns1.getTransport().getListeningPort();
      server.register(factory.getNewInstance("task1"), new InetSocketAddress(nameServerAddr, port1));

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
   *
   * @throws Exception
   */
  @Test
  public void testMessagingNetworkServiceRateDisjoint() throws Exception {
    LOG.log(Level.FINEST, name.getMethodName());

    final IdentifierFactory factory = new StringIdentifierFactory();
    final String nameServerAddr = NetUtils.getLocalAddress();

    final NameServer server = new NameServer(0, factory);
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
            NetworkService<String> ns2 = new NetworkService<String>(
                factory, 0, nameServerAddr, nameServerPort,
                new StringCodec(), new MessagingTransportFactory(),
                new MessageHandler<String>(name2, monitor, numMessages), new ExceptionHandler());
            ns2.registerId(factory.getNewInstance(name2));
            final int port2 = ns2.getTransport().getListeningPort();
            server.register(factory.getNewInstance(name2), new InetSocketAddress(nameServerAddr, port2));

            LOG.log(Level.FINEST, "=== Test network service sender start");
            final String name1 = "task1-" + tt;
            NetworkService<String> ns1 = new NetworkService<String>(
                factory, 0, nameServerAddr, nameServerPort,
                new StringCodec(), new MessagingTransportFactory(),
                new MessageHandler<String>(name1, null, 0), new ExceptionHandler());
            ns1.registerId(factory.getNewInstance(name1));
            final int port1 = ns1.getTransport().getListeningPort();
            server.register(factory.getNewInstance(name1), new InetSocketAddress(nameServerAddr, port1));

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
    String nameServerAddr = NetUtils.getLocalAddress();

    NameServer server = new NameServer(0, factory);
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
      NetworkService<String> ns2 = new NetworkService<String>(
          factory, 0, nameServerAddr, nameServerPort,
          new StringCodec(), new MessagingTransportFactory(),
          new MessageHandler<String>(name2, monitor, totalNumMessages), new ExceptionHandler());
      ns2.registerId(factory.getNewInstance(name2));
      final int port2 = ns2.getTransport().getListeningPort();
      server.register(factory.getNewInstance("task2"), new InetSocketAddress(nameServerAddr, port2));

      LOG.log(Level.FINEST, "=== Test network service sender start");
      final String name1 = "task1";
      NetworkService<String> ns1 = new NetworkService<String>(
          factory, 0, nameServerAddr, nameServerPort,
          new StringCodec(), new MessagingTransportFactory(),
          new MessageHandler<String>(name1, null, 0), new ExceptionHandler());
      ns1.registerId(factory.getNewInstance(name1));
      final int port1 = ns1.getTransport().getListeningPort();
      server.register(factory.getNewInstance("task1"), new InetSocketAddress(nameServerAddr, port1));

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
   *
   * @throws Exception
   */
  @Test
  public void testMessagingNetworkServiceBatchingRate() throws Exception {
    LOG.log(Level.FINEST, name.getMethodName());

    IdentifierFactory factory = new StringIdentifierFactory();
    String nameServerAddr = NetUtils.getLocalAddress();

    NameServer server = new NameServer(0, factory);
    int nameServerPort = server.getPort();

    final int batchSize = 1024 * 1024;
    final int[] messageSizes = {32, 64, 512};

    for (int size : messageSizes) {
      final int numMessages = 300 / (Math.max(1, size / 512));
      final Monitor monitor = new Monitor();

      LOG.log(Level.FINEST, "=== Test network service receiver start");
      // network service
      final String name2 = "task2";
      NetworkService<String> ns2 = new NetworkService<String>(
          factory, 0, nameServerAddr, nameServerPort,
          new StringCodec(), new MessagingTransportFactory(),
          new MessageHandler<String>(name2, monitor, numMessages), new ExceptionHandler());
      ns2.registerId(factory.getNewInstance(name2));
      final int port2 = ns2.getTransport().getListeningPort();
      server.register(factory.getNewInstance("task2"), new InetSocketAddress(nameServerAddr, port2));

      LOG.log(Level.FINEST, "=== Test network service sender start");
      final String name1 = "task1";
      NetworkService<String> ns1 = new NetworkService<String>(
          factory, 0, nameServerAddr, nameServerPort,
          new StringCodec(), new MessagingTransportFactory(),
          new MessageHandler<String>(name1, null, 0), new ExceptionHandler());
      ns1.registerId(factory.getNewInstance(name1));
      final int port1 = ns1.getTransport().getListeningPort();
      server.register(factory.getNewInstance("task1"), new InetSocketAddress(nameServerAddr, port1));

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
   *
   * @param <T> type
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

      //System.out.print(name + " received " + value.getData() + " from " + value.getSrcId() + " to " + value.getDestId());
      for (T obj : value.getData()) {
        // System.out.print(" data: " + obj);
      }
      //LOG.log(Level.FINEST, );
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
