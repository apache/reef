/**
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

import org.apache.reef.io.network.*;
import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.io.network.naming.NameServerImpl;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.services.network.util.Monitor;
import org.apache.reef.services.network.util.NetworkServiceUtil;
import org.apache.reef.services.network.util.StringCodec;
import org.apache.reef.services.network.util.StringNetworkEventHandler;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.Codec;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Network service test
 */
public final class NewNetworkServiceTest {

  private static final Logger LOG = Logger.getLogger(NewNetworkServiceTest.class.getName());

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

    NameServer server = new NameServerImpl(0, factory);
    int nameServerPort = server.getPort();

    final int numMessages = 10;
    final Monitor monitor = new Monitor();

    LOG.log(Level.FINEST, "=== Test network service receiver start");
    // network service
    final String name2 = "task2";
    final Set<Codec<?>> codecs = new HashSet<>();
    codecs.add(new StringCodec());

    StringNetworkEventHandler handler2 = new StringNetworkEventHandler(new MessageHandler<String>(name2, monitor, numMessages));
    Set<NetworkEventHandler<?>> set2 = new HashSet<>();
    set2.add(handler2);

    final Set<String> networkEvents = new HashSet<>();
    networkEvents.add(String.class.getName());

    final org.apache.reef.io.network.NetworkService ns2 = NetworkServiceUtil.getTestNetworkService(
        networkEvents,
        set2,
        codecs,
        null,
        name2,
        nameServerPort
    );

    LOG.log(Level.FINEST, "=== Test network service sender start");
    final String name1 = "task1";
    StringNetworkEventHandler handler1 = new StringNetworkEventHandler(new MessageHandler<String>(name1, null, 0));
    Set<NetworkEventHandler<?>> set1 = new HashSet<>();
    set1.add(handler1);
    final org.apache.reef.io.network.NetworkService ns1 = NetworkServiceUtil.getTestNetworkService(
        networkEvents,
        set1,
        codecs,
        null,
        name1,
        nameServerPort
    );

    final Identifier destId = factory.getNewInstance(name2);

    for (int count = 0; count < numMessages; ++count) {
      ns1.sendEvent(destId, "hello!" + count);
    }

    monitor.mwait();

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

    final Set<Codec<?>> codecs = new HashSet<>();
    codecs.add(new StringCodec());

    final Set<String> networkEvents = new HashSet<>();
    networkEvents.add(String.class.getName());

    NameServer server = new NameServerImpl(0, factory);
    int nameServerPort = server.getPort();

    final int[] messageSizes = {1, 16, 32, 64, 512, 64 * 1024, 1024 * 1024};

    for (int size : messageSizes) {
      final int numMessages = 300000 / (Math.max(1, size / 512));
      final Monitor monitor = new Monitor();

      LOG.log(Level.FINEST, "=== Test network service receiver start");
      // network service
      final String name2 = "task2";
      StringNetworkEventHandler handler2 = new StringNetworkEventHandler(new MessageHandler<String>(name2, monitor, numMessages));
      Set<NetworkEventHandler<?>> set2 = new HashSet<>();
      set2.add(handler2);
      final org.apache.reef.io.network.NetworkService ns2 = NetworkServiceUtil.getTestNetworkService(
          networkEvents,
          set2,
          codecs,
          null,
          name2,
          nameServerPort
      );

      LOG.log(Level.FINEST, "=== Test network service sender start");
      final String name1 = "task1";
      StringNetworkEventHandler handler1 = new StringNetworkEventHandler(new MessageHandler<String>(name1, null, 0));
      Set<NetworkEventHandler<?>> set1 = new HashSet<>();
      set1.add(handler1);
      final org.apache.reef.io.network.NetworkService ns1 = NetworkServiceUtil.getTestNetworkService(
          networkEvents,
          set1,
          codecs,
          null,
          name1,
          nameServerPort
      );

      Identifier destId = factory.getNewInstance(name2);

      // build the message
      StringBuilder msb = new StringBuilder();
      for (int i = 0; i < size; i++) {
        msb.append("1");
      }
      String message = msb.toString();

      long start = System.currentTimeMillis();


      for (int i = 0; i < numMessages; i++) {
        ns1.sendEvent(destId, message);
      }

      monitor.mwait();

      long end = System.currentTimeMillis();
      double runtime = ((double) end - start) / 1000;
      LOG.log(Level.INFO, "size: " + size + "; messages/s: " + numMessages / runtime + " bandwidth(bytes/s): " + ((double) numMessages * 2 * size) / runtime);// x2 for unicode chars

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

    final NameServer server = new NameServerImpl(0, factory);
    final int nameServerPort = server.getPort();

    final Set<Codec<?>> codecs = new HashSet<>();
    codecs.add(new StringCodec());

    BlockingQueue<Object> barrier = new LinkedBlockingQueue<Object>();

    int numThreads = 4;
    final int size = 2000;
    final int numMessages = 30000 / (Math.max(1, size / 512));
    final int totalNumMessages = numMessages * numThreads;

    final Set<String> networkEvents = new HashSet<>();
    networkEvents.add(String.class.getName());

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

            StringNetworkEventHandler handler2 = new StringNetworkEventHandler(new MessageHandler<String>(name2, monitor, numMessages));
            Set<NetworkEventHandler<?>> set2 = new HashSet<>();
            set2.add(handler2);
            final org.apache.reef.io.network.NetworkService ns2 = NetworkServiceUtil.getTestNetworkService(
                networkEvents,
                set2,
                codecs,
                null,
                name2,
                nameServerPort
            );

            LOG.log(Level.FINEST, "=== Test network service sender start");
            final String name1 = "task1-" + tt;
            StringNetworkEventHandler handler1 = new StringNetworkEventHandler(new MessageHandler<String>(name1, null, 0));
            Set<NetworkEventHandler<?>> set1 = new HashSet<>();
            set1.add(handler1);
            final org.apache.reef.io.network.NetworkService ns1 = NetworkServiceUtil.getTestNetworkService(
                networkEvents,
                set1,
                codecs,
                null,
                name1,
                nameServerPort
            );

            Identifier destId = factory.getNewInstance(name2);

            // build the message
            StringBuilder msb = new StringBuilder();
            for (int i = 0; i < size; i++) {
              msb.append("1");
            }
            String message = msb.toString();

            for (int i = 0; i < numMessages; i++) {
              ns1.sendEvent(destId, message);
            }
            monitor.mwait();

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

  /**
   * test multi threaded shared connection rate
   * @throws Exception
   */
  @Test
  public void testMultithreadedSharedConnMessagingNetworkServiceRate() throws Exception {
    LOG.log(Level.FINEST, name.getMethodName());

    IdentifierFactory factory = new StringIdentifierFactory();
    final Set<Codec<?>> codecs = new HashSet<>();
    codecs.add(new StringCodec());

    final Set<String> networkEvents = new HashSet<>();
    networkEvents.add(String.class.getName());

    NameServer server = new NameServerImpl(0, factory);
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
      StringNetworkEventHandler handler2 = new StringNetworkEventHandler(new MessageHandler<String>(name2, monitor, totalNumMessages));
      Set<NetworkEventHandler<?>> set2 = new HashSet<>();
      set2.add(handler2);
      final org.apache.reef.io.network.NetworkService ns2 = NetworkServiceUtil.getTestNetworkService(
          networkEvents,
          set2,
          codecs,
          null,
          name2,
          nameServerPort
      );

      LOG.log(Level.FINEST, "=== Test network service sender start");
      final String name1 = "task1";
      StringNetworkEventHandler handler1 = new StringNetworkEventHandler(new MessageHandler<String>(name1, null, 0));
      Set<NetworkEventHandler<?>> set1 = new HashSet<>();
      set1.add(handler1);
      final org.apache.reef.io.network.NetworkService ns1 = NetworkServiceUtil.getTestNetworkService(
          networkEvents,
          set1,
          codecs,
          null,
          name1,
          nameServerPort
      );

      final Identifier destId = factory.getNewInstance(name2);

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
              for (int i = 0; i < numMessages; i++) {
                ns1.sendEvent(destId, message.toString());
              }
          }
        });
      }

      monitor.mwait();
      e.shutdown();
      e.awaitTermination(30, TimeUnit.SECONDS);


      long end = System.currentTimeMillis();
      double runtime = ((double) end - start) / 1000;

      LOG.log(Level.FINEST, "size: " + size + "; messages/s: " + totalNumMessages / runtime + " bandwidth(bytes/s): " + ((double) totalNumMessages * 2 * size) / runtime);// x2 for unicode chars
      System.out.println( "size: " + size + "; messages/s: " + totalNumMessages / runtime + " bandwidth(bytes/s): " + ((double) totalNumMessages * 2 * size) / runtime);

      ns1.close();
      ns2.close();
    }

    server.close();
  }

  /**
   * Multi sender, multi receiver with multi-thread test
   * @throws Exception
   */
  @Test
  public void testMultithreadedNetworkServiceRate() throws Exception {
    LOG.log(Level.FINEST, name.getMethodName());

    IdentifierFactory factory = new StringIdentifierFactory();

    final Set<Codec<?>> codecs = new HashSet<>();
    codecs.add(new StringCodec());

    NameServer server = new NameServerImpl(0, factory);
    int nameServerPort = server.getPort();

    final int[] messageSizes = {2000};// {1,16,32,64,512,64*1024,1024*1024};

    final Set<String> networkEvents = new HashSet<>();
    networkEvents.add(String.class.getName());

    for (int size : messageSizes) {
      // number of sender thread
      final int numThreads = 3;
      // number of sender network service
      final int senderNum = 2;
      // number of receive network service. same as numThreads for convenience sake
      final int receiverNum = numThreads;
      final int numMessages = (300000 / numThreads / (Math.max(1, size / 512))) * numThreads;
      int totalNumMessages = numMessages * numThreads;
      final Monitor monitor = new Monitor();

      final Identifier[] senders = new Identifier[senderNum];
      final org.apache.reef.io.network.NetworkService[] senderNetworkServices = new org.apache.reef.io.network.NetworkService[senderNum];
      for (int i = 0; i < senderNum; i++) {
        LOG.log(Level.FINEST, "=== Test network service sender" + (i + 1) + "start");
        final String name = "sender" + (i + 1);
        StringNetworkEventHandler handler = new StringNetworkEventHandler(new MessageHandler<String>(name, null, 0));
        Set<NetworkEventHandler<?>> set = new HashSet<>();
        set.add(handler);
        senderNetworkServices[i] = NetworkServiceUtil.getTestNetworkService(
            networkEvents,
            set,
            codecs,
            null,
            name,
            nameServerPort
        );
      }

      final Identifier[] receivers = new Identifier[receiverNum];
      final org.apache.reef.io.network.NetworkService[] receiverNetworkServices = new org.apache.reef.io.network.NetworkService[receiverNum];
      for (int i = 0; i < receiverNum; i++) {
        LOG.log(Level.FINEST, "=== Test network service receiver" + (i + 1) + "start");
        String name = "receiver" + (i + 1);
        receivers[i] = factory.getNewInstance(name);
        StringNetworkEventHandler handler = new StringNetworkEventHandler(new MessageHandler<String>(name, monitor, numMessages));
        Set<NetworkEventHandler<?>> set = new HashSet<>();
        set.add(handler);
        receiverNetworkServices[i] = NetworkServiceUtil.getTestNetworkService(
            networkEvents,
            set,
            codecs,
            null,
            name,
            nameServerPort
        );
      }

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
            int j = 0;
            for (int i = 0; i < numMessages; i++) {
              senderNetworkServices[(j++) % 2].sendEvent(receivers[i % 3], message);
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

      for (int i = 0; i < senderNum; i++) {
        senderNetworkServices[i].close();
      }

      for (int i = 0;i < receiverNum; i++) {
        receiverNetworkServices[i].close();
      }
    }

    server.close();
  }

  /**
   * NetworkService messaging rate benchmark
   *
   * @throws Exception
   */
  @Test
  public void testNetworkServiceBatchingRate() throws Exception {
    LOG.log(Level.FINEST, name.getMethodName());

    IdentifierFactory factory = new StringIdentifierFactory();

    NameServer server = new NameServerImpl(0, factory);
    int nameServerPort = server.getPort();

    final Set<Codec<?>> codecs = new HashSet<>();
    codecs.add(new StringCodec());

    final Set<String> networkEvents = new HashSet<>();
    networkEvents.add(String.class.getName());

    final int batchSize = 1024 * 1024;
    final int[] messageSizes = {32, 64, 512};

    for (int size : messageSizes) {
      final int numMessages = 300 / (Math.max(1, size / 512));
      final Monitor monitor = new Monitor();

      LOG.log(Level.FINEST, "=== Test network service receiver start");
      // network service
      final String name2 = "task2";
      StringNetworkEventHandler handler2 = new StringNetworkEventHandler(new MessageHandler<String>(name2, monitor, numMessages));
      Set<NetworkEventHandler<?>> set2 = new HashSet<>();
      set2.add(handler2);
      final org.apache.reef.io.network.NetworkService ns2 = NetworkServiceUtil.getTestNetworkService(
          networkEvents,
          set2,
          codecs,
          null,
          name2,
          nameServerPort
      );

      LOG.log(Level.FINEST, "=== Test network service sender start");
      final String name1 = "task1";
      StringNetworkEventHandler handler1 = new StringNetworkEventHandler(new MessageHandler<String>(name1, null, 0));
      Set<NetworkEventHandler<?>> set1 = new HashSet<>();
      set1.add(handler1);
      final org.apache.reef.io.network.NetworkService ns1 = NetworkServiceUtil.getTestNetworkService(
          networkEvents,
          set1,
          codecs,
          null,
          name1,
          nameServerPort
      );

      Identifier destId = factory.getNewInstance(name2);

      // build the message
      StringBuilder msb = new StringBuilder();
      for (int i = 0; i < size; i++) {
        msb.append("1");
      }
      String message = msb.toString();

      long start = System.currentTimeMillis();

      for (int i = 0; i < numMessages; i++) {
        StringBuilder sb = new StringBuilder();
        for (int j = 0; j < batchSize / size; j++) {
          sb.append(message);
        }
        ns1.sendEvent(destId, sb.toString());
      }

      monitor.mwait();
      long end = System.currentTimeMillis();
      double runtime = ((double) end - start) / 1000;
      long numAppMessages = numMessages * batchSize / size;
      LOG.log(Level.FINEST, "size: " + size + "; messages/s: " + numAppMessages / runtime + " bandwidth(bytes/s): " + ((double) numAppMessages * 2 * size) / runtime);// x2 for unicode chars

      ns1.close();
      ns2.close();
    }

    server.close();
  }

  /**
   * Sends event list test
   *
   * @throws Exception
   */
  @Test
  public void testSendEventList() throws Exception {
    IdentifierFactory factory = new StringIdentifierFactory();
    NameServer nameServer = new NameServerImpl(0, factory);
    int nameServerPort = nameServer.getPort();

    final Monitor monitor = new Monitor();
    final String sender = "sender";
    final String receiver = "receiver";
    final Identifier receiverId = factory.getNewInstance(receiver);

    final Set<Codec<?>> codecs = new HashSet<>();
    codecs.add(new StringCodec());

    Set<NetworkEventHandler<?>> set = new HashSet<>();
    set.add(new StringNetworkEventHandler(new MessageHandler<String>(sender, null, 0)));

    Set<NetworkLinkListener<?>> exceptionSet = new HashSet<>();
    exceptionSet.add(new TestNetworkLinkListener(monitor));

    final Set<String> networkEvents = new HashSet<>();
    networkEvents.add(String.class.getName());

    final org.apache.reef.io.network.NetworkService senderNS = NetworkServiceUtil.getTestNetworkService(
        networkEvents,
        set,
        codecs,
        exceptionSet,
        sender,
        nameServerPort
    );

    final List<String> events = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      events.add(i + "th message");
    }

    set = new HashSet<>();
    set.add(new NetworkEventHandler<String>() {
      @Override
      public void onNext(NetworkEvent<String> value) {
        if (value.getEventListSize() != events.size()) {
          return;
        }

        for (int i = 0; i < events.size(); i++) {
          if (!events.get(i).equals(value.getEventAt(i))) {
            return;
          }
        }

        monitor.mnotify();
      }
    });

    exceptionSet = new HashSet<>();

    final org.apache.reef.io.network.NetworkService receiverNS = NetworkServiceUtil.getTestNetworkService(
        networkEvents,
        set,
        codecs,
        exceptionSet,
        receiver,
        nameServerPort
    );

    senderNS.sendEventList(receiverId, events);
    monitor.mwait();

    receiverNS.close();
    senderNS.close();
    nameServer.close();
  }

  /**
   * Naming exception test
   *
   * @throws Exception
   */
  @Test
  public void testNamingException() throws Exception {
    IdentifierFactory factory = new StringIdentifierFactory();
    NameServer nameServer = new NameServerImpl(0, factory);
    int nameServerPort = nameServer.getPort();

    final Monitor monitor = new Monitor();
    final String sender = "sender";
    final String receiver = "receiver";
    final Identifier wrongReceiverId = factory.getNewInstance("receive");
    final Identifier receiverId = factory.getNewInstance(receiver);

    final Set<Codec<?>> codecs = new HashSet<>();
    codecs.add(new StringCodec());

    Set<NetworkEventHandler<?>> set = new HashSet<>();
    set.add(new StringNetworkEventHandler(new MessageHandler<String>(sender, null, 0)));

    Set<NetworkLinkListener<?>> exceptionSet = new HashSet<>();
    exceptionSet.add(new TestNetworkLinkListener(monitor));

    final Set<String> networkEvents = new HashSet<>();
    networkEvents.add(String.class.getName());

    final org.apache.reef.io.network.NetworkService senderNS = NetworkServiceUtil.getTestNetworkService(
        networkEvents,
        set,
        codecs,
        exceptionSet,
        sender,
        nameServerPort
    );

    set = new HashSet<>();
    set.add(new StringNetworkEventHandler(new MessageHandler<String>(receiver, null, 0)));

    exceptionSet = new HashSet<>();

    final org.apache.reef.io.network.NetworkService receiverNS = NetworkServiceUtil.getTestNetworkService(
        networkEvents,
        set,
        codecs,
        exceptionSet,
        receiver,
        nameServerPort
    );

    new Thread(new Runnable() {
      @Override
      public void run() {
        senderNS.sendEvent(wrongReceiverId, "Test message.");
      }
    }).start();

    monitor.mwait();
    receiverNS.close();
    senderNS.close();
    nameServer.close();
  }

  /**
   * Test exception handler
   */
  class TestNetworkLinkListener implements NetworkLinkListener<String> {

    private Monitor monitor;

    TestNetworkLinkListener(Monitor monitor) {
      this.monitor = monitor;
    }

    @Override
    public void onSuccess(Identifier remoteId, List<String> messageList) {
      LOG.log(Level.FINE, "TestNetworkExceptionHandler - onSuccess " + messageList);
    }

    @Override
    public void onException(Throwable cause, Identifier remoteId, List<String> messageList) {
      LOG.log(Level.FINE, "Network exception occurred. " + cause + ", " + messageList + ", " + remoteId);
      monitor.mnotify();
    }
  }

  /**
   * Test message handler
   *
   * @param <T> type
   */
  class MessageHandler<T> implements EventHandler<T> {

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
    public void onNext(T value) {
      count.incrementAndGet();

      if (count.get() == expected) {
        monitor.mnotify();
      }
    }
  }
}