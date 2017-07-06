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
package org.apache.reef.wake.test.avro;

import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.avro.ProtocolSerializer;
import org.apache.reef.wake.impl.LoggingEventHandler;
import org.apache.reef.wake.impl.LoggingUtils;
import org.apache.reef.wake.impl.MultiObserverImpl;
import org.apache.reef.wake.remote.*;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.remote.impl.ByteCodec;
import org.apache.reef.wake.remote.ports.TcpPortProvider;
import org.apache.reef.wake.test.avro.message.AvroTestMessage;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;

/**
 *  Verify the protocol serializer can serialize and deserialize messages
 *  exchanged between two remote manager classes.
 */
public class ProtocolSerializerTest {
  private static final Logger LOG = Logger.getLogger(ProtocolSerializer.class.getName());
  private static final String LOG_PREFIX = "TEST ";

  public ProtocolSerializerTest() {
  }

  @Rule
  public final TestName name = new TestName();

  /**
   * Verify Avro message can be serialized and deserialized
   * between two remote managers.
   */
  @Test
  public void testProtocolSerializerTest() throws Exception {
    System.out.println(LOG_PREFIX + name.getMethodName());
    LoggingUtils.setLoggingLevel(Level.INFO);

    final int[] numbers = {12, 25};
    final String[] strings = {"The first string", "The second string"};

    // Queues for storing messages byte messages.
    final BlockingQueue<byte[]> queue1 = new LinkedBlockingQueue<>();
    final BlockingQueue<byte[]> queue2 = new LinkedBlockingQueue<>();

    // Remote managers for sending and receiving byte messages.
    final RemoteManager remoteManager1 = getTestRemoteManager("RemoteManagerOne");
    final RemoteManager remoteManager2 = getTestRemoteManager("RemoteManagerTwo");

    // Register message handlers for byte level messages.
    remoteManager1.registerHandler(byte[].class, new ByteMessageObserver(queue1));
    remoteManager2.registerHandler(byte[].class, new ByteMessageObserver(queue2));

    final RemoteIdentifier remoteIdentifier1 = remoteManager1.getMyIdentifier();
    final RemoteIdentifier remoteIdentifier2 = remoteManager2.getMyIdentifier();

    final EventHandler<byte[]> sender1 = remoteManager1.getHandler(remoteIdentifier2, byte[].class);
    final EventHandler<byte[]> sender2 = remoteManager2.getHandler(remoteIdentifier1, byte[].class);

    final ProtocolSerializer serializer = new ProtocolSerializer("org.apache.reef.wake.test.avro.message");

    sender1.onNext(serializer.write(new AvroTestMessage(numbers[0], strings[0]), 1));
    sender2.onNext(serializer.write(new AvroTestMessage(numbers[1], strings[1]), 2));

    final AvroMessageObserver avroObserver1 = new AvroMessageObserver();
    final AvroMessageObserver avroObserver2 = new AvroMessageObserver();

    serializer.read(queue1.take(), avroObserver1);
    serializer.read(queue2.take(), avroObserver2);

    assertEquals(numbers[0], avroObserver2.number);
    assertEquals(strings[0], avroObserver2.data);

    assertEquals(numbers[1], avroObserver1.number);
    assertEquals(strings[1], avroObserver1.data);
  }

  /**
   * Build a remote manager on the local IP address with an unused port.
   * @param identifier The identifier of the remote manager.
   * @return A RemoteManager instance listing on the local IP address
   *         with a unique port number.
   */
  private RemoteManager getTestRemoteManager(final String identifier) {
    RemoteManager remoteManager = null;
    try {
      int port = 0;
      boolean order = true;
      int retries = 3;
      int timeOut = 10000;

      final Injector injector = Tang.Factory.getTang().newInjector();
      final LocalAddressProvider localAddressProvider = injector.getInstance(LocalAddressProvider.class);
      final TcpPortProvider tcpPortProvider = injector.getInstance(TcpPortProvider.class);
      final RemoteManagerFactory remoteManagerFactory = injector.getInstance(RemoteManagerFactory.class);

      remoteManager = remoteManagerFactory.getInstance(
        identifier, localAddressProvider.getLocalAddress(), port, new ByteCodec(),
        new LoggingEventHandler<Throwable>(), order, retries, timeOut,
        localAddressProvider, tcpPortProvider);

    } catch (final Exception e) {
      e.printStackTrace();
      LOG.log(Level.SEVERE, "Initialization failed: " + e.getMessage());
    }
    return remoteManager;
  }

  private final class ByteMessageObserver implements EventHandler<RemoteMessage<byte[]>> {
    private BlockingQueue<byte[]> queue;

    /**
     * @param queue Queue where incoming messages will be stored.
     */
    ByteMessageObserver(final BlockingQueue<byte[]> queue) {
      this.queue = queue;
    }

    /**
     * Deserialize and direct incoming messages to the registered MuiltiObserver event handler.
     * @param message A RemoteMessage<byte[]> object which will be deserialized.
     */
    public void onNext(final RemoteMessage<byte[]> message) {
      queue.add(message.getMessage());
    }
  }

  /**
   * Processes messages from the network remote manager.
   */
  public final class AvroMessageObserver extends MultiObserverImpl<AvroMessageObserver> {
    public int number;
    public String data;

    public void onError(final Exception error) {
      LOG.log(Level.SEVERE, "OnError: ", error.getMessage());
    }

    public void onCompleted() {
      LOG.log(Level.INFO, "OnCompleted");
    }

    /**
     * Processes protocol messages from the C# side of the bridge.
     * @param identifier A long value which is the unique message identifier.
     * @param message A reference to the received avro test message.
     */
    public void onNext(final long identifier, final AvroTestMessage message) {
      number = message.getNumber();
      data = message.getData().toString();
    }
  }
}
