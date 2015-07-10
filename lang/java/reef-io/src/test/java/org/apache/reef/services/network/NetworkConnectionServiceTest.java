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
import org.apache.reef.io.network.util.StringCodec;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.services.network.util.*;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.Codec;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.remote.address.LocalAddressProviderFactory;
import org.apache.reef.wake.remote.impl.ObjectSerializableCodec;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Default Network connection service test.
 */
public class NetworkConnectionServiceTest {
  private static final Logger LOG = Logger.getLogger(NetworkConnectionServiceTest.class.getName());

  private final LocalAddressProvider localAddressProvider;
  private final String localAddress;
  private final Identifier groupCommClientId;
  private final Identifier shuffleClientId;

  public NetworkConnectionServiceTest() throws InjectionException {
    localAddressProvider = LocalAddressProviderFactory.getInstance();
    localAddress = localAddressProvider.getLocalAddress();

    final IdentifierFactory idFac = new StringIdentifierFactory();
    this.groupCommClientId = idFac.getNewInstance("groupComm");
    this.shuffleClientId = idFac.getNewInstance("shuffle");
  }

  @Rule
  public TestName name = new TestName();

  private void runMessagingNetworkConnectionService(Codec<String> codec) throws Exception {
    final int numMessages = 2000;
    final Monitor monitor = new Monitor();
    final NetworkMessagingTest messagingTest = new NetworkMessagingTest(localAddress);
    messagingTest.registerTestConnectionFactory(groupCommClientId, numMessages, monitor, codec);

    final Connection<String> conn = messagingTest.getConnectionFromSenderToReceiver(groupCommClientId);
    try {
      conn.open();
      for (int count = 0; count < numMessages; ++count) {
        // send messages to the receiver.
        conn.write("hello" + count);
      }
      monitor.mwait();
    } catch (NetworkException e) {
      e.printStackTrace();
    }

    conn.close();
    messagingTest.close();
  }

  /**
   * NetworkConnectionService messaging test.
   */
  @Test
  public void testMessagingNetworkConnectionService() throws Exception {
    LOG.log(Level.FINEST, name.getMethodName());
    runMessagingNetworkConnectionService(new StringCodec());
  }

  /**
   * NetworkConnectionService streaming messaging test.
   */
  @Test
  public void testStreamingMessagingNetworkConnectionService() throws Exception {
    LOG.log(Level.FINEST, name.getMethodName());
    runMessagingNetworkConnectionService(new StreamingStringCodec());
  }

  public void runNetworkConnServiceWithMultipleConnFactories(Codec<String> stringCodec, Codec<Integer> integerCodec) throws Exception {
    final ExecutorService executor = Executors.newFixedThreadPool(5);

    final int groupcommMessages = 1000;
    final Monitor monitor = new Monitor();
    final NetworkMessagingTest messagingTest = new NetworkMessagingTest(localAddress);

    messagingTest.registerTestConnectionFactory(groupCommClientId, groupcommMessages, monitor, stringCodec);

    final int shuffleMessges = 2000;
    final Monitor monitor2 = new Monitor();
    messagingTest.registerTestConnectionFactory(shuffleClientId, shuffleMessges, monitor2, integerCodec);

    executor.submit(new Runnable() {
      @Override
      public void run() {
        final Connection<String> conn = messagingTest.getConnectionFromSenderToReceiver(groupCommClientId);
        try {
          conn.open();
          for (int count = 0; count < groupcommMessages; ++count) {
            // send messages to the receiver.
            conn.write("hello" + count);
          }
          monitor.mwait();
          conn.close();
        } catch (Exception e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      }
    });

    executor.submit(new Runnable() {
      @Override
      public void run() {
        final Connection<Integer> conn = messagingTest.getConnectionFromSenderToReceiver(shuffleClientId);
        try {
          conn.open();
          for (int count = 0; count < shuffleMessges; ++count) {
            // send messages to the receiver.
            conn.write(count);
          }
          monitor2.mwait();
          conn.close();
        } catch (Exception e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      }
    });

    monitor.mwait();
    monitor2.mwait();
    executor.shutdown();
    messagingTest.close();
  }

  /**
   * Test NetworkService registering multiple connection factories.
   */
  @Test
  public void testMultipleConnectionFactoriesTest() throws Exception {
    LOG.log(Level.FINEST, name.getMethodName());
    runNetworkConnServiceWithMultipleConnFactories(new StringCodec(), new ObjectSerializableCodec<Integer>());
  }

  /**
   * Test NetworkService registering multiple connection factories with Streamingcodec.
   */
  @Test
  public void testMultipleConnectionFactoriesStreamingTest() throws Exception {
    LOG.log(Level.FINEST, name.getMethodName());
    runNetworkConnServiceWithMultipleConnFactories(new StreamingStringCodec(), new StreamingIntegerCodec());
  }

  /**
   * NetworkService messaging rate benchmark.
   */
  @Test
  public void testMessagingNetworkConnServiceRate() throws Exception {
    LOG.log(Level.FINEST, name.getMethodName());
    final int[] messageSizes = {1, 16, 32, 64, 512, 64 * 1024, 1024 * 1024};

    for (int size : messageSizes) {
      final int numMessages = 300000 / (Math.max(1, size / 512));
      final Monitor monitor = new Monitor();
      final Codec<String> codec = new StringCodec();
      final NetworkMessagingTest messagingTest = new NetworkMessagingTest(localAddress);
      messagingTest.registerTestConnectionFactory(groupCommClientId, numMessages, monitor, codec);
      final Connection<String> conn = messagingTest.getConnectionFromSenderToReceiver(groupCommClientId);

      // build the message
      final StringBuilder msb = new StringBuilder();
      for (int i = 0; i < size; i++) {
        msb.append("1");
      }
      final String message = msb.toString();

      long start = System.currentTimeMillis();
      try {
        conn.open();
        for (int count = 0; count < numMessages; ++count) {
          // send messages to the receiver.
          conn.write(message);
        }
        monitor.mwait();
      } catch (NetworkException e) {
        e.printStackTrace();
      }
      final long end = System.currentTimeMillis();

      final double runtime = ((double) end - start) / 1000;
      LOG.log(Level.INFO, "size: " + size + "; messages/s: " + numMessages / runtime + " bandwidth(bytes/s): " + ((double) numMessages * 2 * size) / runtime);// x2 for unicode chars
      messagingTest.close();
    }
  }

  /**
   * NetworkService messaging rate benchmark.
   */
  @Test
  public void testMessagingNetworkConnServiceRateDisjoint() throws Exception {
    LOG.log(Level.FINEST, name.getMethodName());
    final BlockingQueue<Object> barrier = new LinkedBlockingQueue<Object>();

    final int numThreads = 4;
    final int size = 2000;
    final int numMessages = 300000 / (Math.max(1, size / 512));
    final int totalNumMessages = numMessages * numThreads;

    final ExecutorService e = Executors.newCachedThreadPool();
    for (int t = 0; t < numThreads; t++) {
      final int tt = t;

      e.submit(new Runnable() {
        public void run() {
          try {
            final Monitor monitor = new Monitor();
            final Codec<String> codec = new StringCodec();
            final NetworkMessagingTest messagingTest = new NetworkMessagingTest(localAddress);
            messagingTest.registerTestConnectionFactory(groupCommClientId, numMessages, monitor, codec);
            final Connection<String> conn = messagingTest.getConnectionFromSenderToReceiver(groupCommClientId);

            // build the message
            final StringBuilder msb = new StringBuilder();
            for (int i = 0; i < size; i++) {
              msb.append("1");
            }
            final String message = msb.toString();

            try {
              conn.open();
              for (int count = 0; count < numMessages; ++count) {
                // send messages to the receiver.
                conn.write(message);
              }
              monitor.mwait();
            } catch (NetworkException e) {
              e.printStackTrace();
            }
            messagingTest.close();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      });
    }

    // start and time
    final long start = System.currentTimeMillis();
    final Object ignore = new Object();
    for (int i = 0; i < numThreads; i++) barrier.add(ignore);
    e.shutdown();
    e.awaitTermination(100, TimeUnit.SECONDS);
    final long end = System.currentTimeMillis();
    final double runtime = ((double) end - start) / 1000;
    LOG.log(Level.INFO, "size: " + size + "; messages/s: " + totalNumMessages / runtime + " bandwidth(bytes/s): " + ((double) totalNumMessages * 2 * size) / runtime);// x2 for unicode chars
  }

  @Test
  public void testMultithreadedSharedConnMessagingNetworkConnServiceRate() throws Exception {
    LOG.log(Level.FINEST, name.getMethodName());
    final int[] messageSizes = {2000};// {1,16,32,64,512,64*1024,1024*1024};

    for (int size : messageSizes) {
      final int numMessages = 300000 / (Math.max(1, size / 512));
      final int numThreads = 2;
      final int totalNumMessages = numMessages * numThreads;
      final Monitor monitor = new Monitor();
      final Codec<String> codec = new StringCodec();
      final NetworkMessagingTest messagingTest = new NetworkMessagingTest(localAddress);
      messagingTest.registerTestConnectionFactory(groupCommClientId, totalNumMessages, monitor, codec);

      final ExecutorService e = Executors.newCachedThreadPool();

      // build the message
      final StringBuilder msb = new StringBuilder();
      for (int i = 0; i < size; i++) {
        msb.append("1");
      }
      final String message = msb.toString();
      final Connection<String> conn = messagingTest.getConnectionFromSenderToReceiver(groupCommClientId);

      final long start = System.currentTimeMillis();
      for (int i = 0; i < numThreads; i++) {
        e.submit(new Runnable() {
          @Override
          public void run() {

              try {
                conn.open();
                for (int count = 0; count < numMessages; ++count) {
                  // send messages to the receiver.
                  conn.write(message);
                }
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
          }
        });
      }

      e.shutdown();
      e.awaitTermination(30, TimeUnit.SECONDS);
      monitor.mwait();
      final long end = System.currentTimeMillis();
      final double runtime = ((double) end - start) / 1000;
      LOG.log(Level.INFO, "size: " + size + "; messages/s: " + totalNumMessages / runtime + " bandwidth(bytes/s): " + ((double) totalNumMessages * 2 * size) / runtime);// x2 for unicode chars
      conn.close();
      messagingTest.close();
    }
  }

  /**
   * NetworkService messaging rate benchmark.
   */
  @Test
  public void testMessagingNetworkConnServiceBatchingRate() throws Exception {
    LOG.log(Level.FINEST, name.getMethodName());

    final int batchSize = 1024 * 1024;
    final int[] messageSizes = {32, 64, 512};

    for (int size : messageSizes) {
      final int numMessages = 300 / (Math.max(1, size / 512));
      final Monitor monitor = new Monitor();
      final Codec<String> codec = new StringCodec();
      final NetworkMessagingTest messagingTest = new NetworkMessagingTest(localAddress);
      messagingTest.registerTestConnectionFactory(groupCommClientId, numMessages, monitor, codec);
      final Connection<String> conn = messagingTest.getConnectionFromSenderToReceiver(groupCommClientId);

      // build the message
      final StringBuilder msb = new StringBuilder();
      for (int i = 0; i < size; i++) {
        msb.append("1");
      }
      final String message = msb.toString();

      final long start = System.currentTimeMillis();
      try {
        for (int i = 0; i < numMessages; i++) {
          final StringBuilder sb = new StringBuilder();
          for (int j = 0; j < batchSize / size; j++) {
            sb.append(message);
          }
          conn.open();
          conn.write(sb.toString());
        }
        monitor.mwait();
      } catch (NetworkException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }

      final long end = System.currentTimeMillis();
      final double runtime = ((double) end - start) / 1000;
      final long numAppMessages = numMessages * batchSize / size;
      LOG.log(Level.INFO, "size: " + size + "; messages/s: " + numAppMessages / runtime + " bandwidth(bytes/s): " + ((double) numAppMessages * 2 * size) / runtime);// x2 for unicode chars
      messagingTest.close();
    }
  }
}