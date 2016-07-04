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
package org.apache.reef.wake.test.remote;

import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.impl.LoggingEventHandler;
import org.apache.reef.wake.impl.LoggingUtils;
import org.apache.reef.wake.impl.MultiEventHandler;
import org.apache.reef.wake.impl.TimerStage;
import org.apache.reef.wake.remote.Decoder;
import org.apache.reef.wake.remote.Encoder;
import org.apache.reef.wake.remote.RemoteIdentifier;
import org.apache.reef.wake.remote.RemoteIdentifierFactory;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.remote.impl.*;
import org.apache.reef.wake.remote.transport.Transport;
import org.apache.reef.wake.remote.transport.TransportFactory;
import org.apache.reef.wake.test.util.Monitor;
import org.apache.reef.wake.test.util.TimeoutHandler;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.logging.Level;

/**
 * Tests for remote event.
 */
public class RemoteTest {
  private final LocalAddressProvider localAddressProvider;
  private final TransportFactory tpFactory;
  @Rule
  public final TestName name = new TestName();

  private static final String LOG_PREFIX = "TEST ";


  public RemoteTest() throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector();
    this.localAddressProvider = injector.getInstance(LocalAddressProvider.class);
    this.tpFactory = injector.getInstance(TransportFactory.class);
  }

  @Test
  public void testRemoteEventCodec() throws UnknownHostException {
    System.out.println(LOG_PREFIX + name.getMethodName());

    final ObjectSerializableCodec<TestEvent> codec = new ObjectSerializableCodec<>();

    final RemoteEventCodec<TestEvent> reCodec = new RemoteEventCodec<>(codec);
    final SocketAddress localAddr = new InetSocketAddress(this.localAddressProvider.getLocalAddress(), 8000);
    final SocketAddress remoteAddr = new InetSocketAddress(this.localAddressProvider.getLocalAddress(), 9000);

    final RemoteEvent<TestEvent> e1 = new RemoteEvent<>(
        localAddr, remoteAddr, 1, new TestEvent("hello", 0.0));
    System.out.println(e1);

    final byte[] data = reCodec.encode(e1);
    final RemoteEvent<TestEvent> e2 = reCodec.decode(data);
    System.out.println(e2);

    Assert.assertEquals(e1.getEvent().getMessage(), e2.getEvent().getMessage());
  }

  @Test
  public void testRandomPort() throws Exception {
    System.out.println(LOG_PREFIX + name.getMethodName());
    LoggingUtils.setLoggingLevel(Level.FINEST);

    // receiver stage
    final RemoteReceiverStage reRecvStage = new RemoteReceiverStage(null, null, 10);

    final String hostAddress = this.localAddressProvider.getLocalAddress();

    // transport
    final Transport transport1 = tpFactory.newInstance(hostAddress, 0, reRecvStage, reRecvStage, 1, 10000);
    final int port1 = transport1.getListeningPort();

    final Transport transport2 = tpFactory.newInstance(hostAddress, 0, reRecvStage, reRecvStage, 1, 10000);
    final int port2 = transport2.getListeningPort();

    transport1.close();
    transport2.close();

    Assert.assertFalse("Two random ports are the same", port1 == port2);
  }

  @Test
  public void testRemoteTest() throws Exception {
    System.out.println(LOG_PREFIX + name.getMethodName());
    LoggingUtils.setLoggingLevel(Level.FINEST);

    final Monitor monitor = new Monitor();
    final TimerStage timer = new TimerStage(new TimeoutHandler(monitor), 5000, 5000);

    // receiver stage
    // decoder map
    final Map<Class<?>, Decoder<?>> clazzToDecoderMap = new HashMap<>();
    clazzToDecoderMap.put(TestEvent.class, new ObjectSerializableCodec<TestEvent>());
    clazzToDecoderMap.put(TestEvent2.class, new ObjectSerializableCodec<TestEvent2>());
    final Decoder<Object> decoder = new MultiDecoder<>(clazzToDecoderMap);

    // receive handlers
    final int finalSize = 6; // 6 events will be sent
    final Map<Class<?>, EventHandler<?>> clazzToHandlerMap = new HashMap<>();
    final Set<Object> set = Collections.synchronizedSet(new HashSet<>());
    clazzToHandlerMap.put(TestEvent.class, new ConsoleEventHandler<TestEvent>("recvEH1", set, finalSize, monitor));
    clazzToHandlerMap.put(TestEvent2.class, new ConsoleEventHandler<TestEvent2>("recvEH2", set, finalSize, monitor));
    final EventHandler<Object> handler = new MultiEventHandler<>(clazzToHandlerMap);

    // receiver stage
    final RemoteReceiverStage reRecvStage = new RemoteReceiverStage(
        new RemoteEventHandler(decoder, handler), new LoggingEventHandler<Throwable>(), 10);

    final String hostAddress = this.localAddressProvider.getLocalAddress();

    // transport
    final Transport transport = tpFactory.newInstance(hostAddress, 0, reRecvStage, reRecvStage, 1, 10000);

    // port
    final int port = transport.getListeningPort();

    // mux encoder with encoder map
    final Map<Class<?>, Encoder<?>> clazzToEncoderMap = new HashMap<>();
    clazzToEncoderMap.put(TestEvent.class, new ObjectSerializableCodec<TestEvent>());
    clazzToEncoderMap.put(TestEvent2.class, new ObjectSerializableCodec<TestEvent2>());
    final Encoder<Object> encoder = new MultiEncoder<>(clazzToEncoderMap);

    // sender stage
    final RemoteSenderStage reSendStage = new RemoteSenderStage(encoder, transport, 10);

    final RemoteIdentifierFactory factory = new DefaultRemoteIdentifierFactoryImplementation();
    final RemoteIdentifier myId = factory.getNewInstance("socket://" + hostAddress + ":" + 8000);
    final RemoteIdentifier remoteId = factory.getNewInstance("socket://" + hostAddress + ":" + port);

    final RemoteSeqNumGenerator seqGen = new RemoteSeqNumGenerator();
    // proxy handler for a remotely running handler
    final ProxyEventHandler<TestEvent> proxyHandler1 = new ProxyEventHandler<>(
        myId, remoteId, "recvEH1", reSendStage.<TestEvent>getHandler(), seqGen);
    proxyHandler1.onNext(new TestEvent("hello1", 0.0));
    proxyHandler1.onNext(new TestEvent("hello2", 0.0));

    final ProxyEventHandler<TestEvent2> proxyHandler2 = new ProxyEventHandler<>(
        myId, remoteId, "recvEH2", reSendStage.<TestEvent2>getHandler(), seqGen);
    proxyHandler2.onNext(new TestEvent2("hello1", 1.0));
    proxyHandler2.onNext(new TestEvent2("hello2", 1.0));

    final ProxyEventHandler<TestEvent> proxyHandler3 = new ProxyEventHandler<>(
        myId, remoteId, "recvEH3", reSendStage.<TestEvent>getHandler(), seqGen);
    proxyHandler3.onNext(new TestEvent("hello1", 1.0));
    proxyHandler3.onNext(new TestEvent("hello2", 1.0));

    monitor.mwait();

    if (set.size() != finalSize) {
      Assert.fail(name.getMethodName() + " takes too long and times out : " +
          set.size() + " out of " + finalSize + " events");
    }

    // shutdown
    reSendStage.close();
    reRecvStage.close();
    transport.close();
    timer.close();
  }

  class RemoteEventHandler implements EventHandler<RemoteEvent<byte[]>> {

    private final Decoder<Object> decoder;
    private final EventHandler<Object> handler;

    RemoteEventHandler(final Decoder<Object> decoder, final EventHandler<Object> handler) {
      this.decoder = decoder;
      this.handler = handler;
    }

    @Override
    public void onNext(final RemoteEvent<byte[]> value) {
      handler.onNext(decoder.decode(value.getEvent()));
    }
  }

  class ConsoleEventHandler<T> implements EventHandler<T> {

    private final String name;
    private final Set<Object> set;
    private final int finalSize;
    private final Monitor monitor;

    ConsoleEventHandler(final String name, final Set<Object> set, final int finalSize, final Monitor monitor) {
      this.name = name;
      this.set = set;
      this.finalSize = finalSize;
      this.monitor = monitor;
    }

    @Override
    public void onNext(final T value) {
      System.out.println(name + " " + value);
      set.add(value);
      if (set.size() == finalSize) {
        monitor.mnotify();
      }
    }
  }
}
