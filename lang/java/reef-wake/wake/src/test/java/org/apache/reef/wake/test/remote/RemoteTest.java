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
package org.apache.reef.wake.test.remote;

import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.impl.LoggingEventHandler;
import org.apache.reef.wake.impl.LoggingUtils;
import org.apache.reef.wake.impl.MultiEventHandler;
import org.apache.reef.wake.impl.TimerStage;
import org.apache.reef.wake.remote.*;
import org.apache.reef.wake.remote.impl.*;
import org.apache.reef.wake.remote.transport.Transport;
import org.apache.reef.wake.remote.transport.netty.NettyMessagingTransport;
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

public class RemoteTest {

  @Rule
  public final TestName name = new TestName();

  final String logPrefix = "TEST ";

  @Test
  public void testRemoteEventCodec() throws UnknownHostException {
    System.out.println(logPrefix + name.getMethodName());

    ObjectSerializableCodec<TestEvent> codec = new ObjectSerializableCodec<TestEvent>();

    RemoteEventCodec<TestEvent> reCodec = new RemoteEventCodec<TestEvent>(codec);
    SocketAddress localAddr = new InetSocketAddress(NetUtils.getLocalAddress(), 8000);
    SocketAddress remoteAddr = new InetSocketAddress(NetUtils.getLocalAddress(), 9000);

    RemoteEvent<TestEvent> e1 = new RemoteEvent<TestEvent>(
        localAddr, remoteAddr, "stage1", "stage2", 1, new TestEvent("hello", 0.0));
    System.out.println(e1);

    byte[] data = reCodec.encode(e1);
    RemoteEvent<TestEvent> e2 = reCodec.decode(data);
    System.out.println(e2);

    Assert.assertEquals(e1.getSink(), e2.getSink());
    Assert.assertEquals(e1.getEvent().getMessage(), e2.getEvent().getMessage());
  }

  @Test
  public void testRandomPort() throws Exception {
    System.out.println(logPrefix + name.getMethodName());
    LoggingUtils.setLoggingLevel(Level.FINEST);

    // receiver stage
    final RemoteReceiverStage reRecvStage = new RemoteReceiverStage(null, null, 10);

    String hostAddress = NetUtils.getLocalAddress();

    // transport
    Transport transport1 = new NettyMessagingTransport(hostAddress, 0, reRecvStage, reRecvStage, 1, 10000);
    int port1 = transport1.getListeningPort();

    Transport transport2 = new NettyMessagingTransport(hostAddress, 0, reRecvStage, reRecvStage, 1, 10000);
    int port2 = transport2.getListeningPort();

    transport1.close();
    transport2.close();

    Assert.assertFalse("Two random ports are the same", port1 == port2);
  }

  @Test
  public void testRemoteTest() throws Exception {
    System.out.println(logPrefix + name.getMethodName());
    LoggingUtils.setLoggingLevel(Level.FINEST);

    Monitor monitor = new Monitor();
    TimerStage timer = new TimerStage(new TimeoutHandler(monitor), 5000, 5000);

    // port
    int port = 9101;

    // receiver stage
    // decoder map
    Map<Class<?>, Decoder<?>> clazzToDecoderMap = new HashMap<Class<?>, Decoder<?>>();
    clazzToDecoderMap.put(TestEvent.class, new ObjectSerializableCodec<TestEvent>());
    clazzToDecoderMap.put(TestEvent2.class, new ObjectSerializableCodec<TestEvent2>());
    Decoder<Object> decoder = new MultiDecoder<Object>(clazzToDecoderMap);

    // receive handlers
    int finalSize = 6; // 6 events will be sent
    Map<Class<?>, EventHandler<?>> clazzToHandlerMap = new HashMap<Class<?>, EventHandler<?>>();
    Set<Object> set = Collections.synchronizedSet(new HashSet<Object>());
    clazzToHandlerMap.put(TestEvent.class, new ConsoleEventHandler<TestEvent>("recvEH1", set, finalSize, monitor));
    clazzToHandlerMap.put(TestEvent2.class, new ConsoleEventHandler<TestEvent2>("recvEH2", set, finalSize, monitor));
    EventHandler<Object> handler = new MultiEventHandler<Object>(clazzToHandlerMap);

    // receiver stage
    final RemoteReceiverStage reRecvStage = new RemoteReceiverStage(
        new RemoteEventHandler(decoder, handler), new LoggingEventHandler<Throwable>(), 10);

    String hostAddress = NetUtils.getLocalAddress();

    // transport
    Transport transport = new NettyMessagingTransport(hostAddress, port, reRecvStage, reRecvStage, 1, 10000);

    // mux encoder with encoder map
    Map<Class<?>, Encoder<?>> clazzToEncoderMap = new HashMap<Class<?>, Encoder<?>>();
    clazzToEncoderMap.put(TestEvent.class, new ObjectSerializableCodec<TestEvent>());
    clazzToEncoderMap.put(TestEvent2.class, new ObjectSerializableCodec<TestEvent2>());
    Encoder<Object> encoder = new MultiEncoder<Object>(clazzToEncoderMap);

    // sender stage
    final RemoteSenderStage reSendStage = new RemoteSenderStage(encoder, transport, 10);

    RemoteIdentifierFactory factory = new DefaultRemoteIdentifierFactoryImplementation();
    RemoteIdentifier myId = factory.getNewInstance("socket://" + hostAddress + ":" + 8000);
    RemoteIdentifier remoteId = factory.getNewInstance("socket://" + hostAddress + ":" + port);

    RemoteSeqNumGenerator seqGen = new RemoteSeqNumGenerator();
    // proxy handler for a remotely running handler
    ProxyEventHandler<TestEvent> proxyHandler1 = new ProxyEventHandler<TestEvent>(
        myId, remoteId, "recvEH1", reSendStage.<TestEvent>getHandler(), seqGen);
    proxyHandler1.onNext(new TestEvent("hello1", 0.0));
    proxyHandler1.onNext(new TestEvent("hello2", 0.0));

    ProxyEventHandler<TestEvent2> proxyHandler2 = new ProxyEventHandler<TestEvent2>(
        myId, remoteId, "recvEH2", reSendStage.<TestEvent2>getHandler(), seqGen);
    proxyHandler2.onNext(new TestEvent2("hello1", 1.0));
    proxyHandler2.onNext(new TestEvent2("hello2", 1.0));

    ProxyEventHandler<TestEvent> proxyHandler3 = new ProxyEventHandler<TestEvent>(
        myId, remoteId, "recvEH3", reSendStage.<TestEvent>getHandler(), seqGen);
    proxyHandler3.onNext(new TestEvent("hello1", 1.0));
    proxyHandler3.onNext(new TestEvent("hello2", 1.0));

    monitor.mwait();

    if (set.size() != finalSize)
      Assert.fail(name.getMethodName() + " takes too long and times out : " + set.size() + " out of " + finalSize + " events");

    // shutdown
    reSendStage.close();
    reRecvStage.close();
    transport.close();
    timer.close();
  }

  class RemoteEventHandler implements EventHandler<RemoteEvent<byte[]>> {

    private final Decoder<Object> decoder;
    private final EventHandler<Object> handler;

    RemoteEventHandler(Decoder<Object> decoder, EventHandler<Object> handler) {
      this.decoder = decoder;
      this.handler = handler;
    }

    @Override
    public void onNext(RemoteEvent<byte[]> value) {
      handler.onNext(decoder.decode(value.getEvent()));
    }
  }

  class ConsoleEventHandler<T> implements EventHandler<T> {

    private final String name;
    private final Set<Object> set;
    private final int finalSize;
    private final Monitor monitor;

    ConsoleEventHandler(String name, Set<Object> set, int finalSize, Monitor monitor) {
      this.name = name;
      this.set = set;
      this.finalSize = finalSize;
      this.monitor = monitor;
    }

    @Override
    public void onNext(T value) {
      System.out.println(name + " " + value);
      set.add(value);
      if (set.size() == finalSize) {
        monitor.mnotify();
      }
    }
  }
}
