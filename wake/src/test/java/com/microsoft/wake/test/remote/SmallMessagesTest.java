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
package com.microsoft.wake.test.remote;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

import org.junit.Assert;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import com.microsoft.wake.EventHandler;
import com.microsoft.wake.impl.LoggingUtils;
import com.microsoft.wake.impl.MultiEventHandler;
import com.microsoft.wake.impl.TimerStage;
import com.microsoft.wake.remote.Decoder;
import com.microsoft.wake.remote.Encoder;
import com.microsoft.wake.remote.NetUtils;
import com.microsoft.wake.remote.RemoteIdentifier;
import com.microsoft.wake.remote.RemoteIdentifierFactory;
import com.microsoft.wake.remote.impl.DefaultRemoteIdentifierFactoryImplementation;
import com.microsoft.wake.remote.impl.MultiDecoder;
import com.microsoft.wake.remote.impl.MultiEncoder;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;
import com.microsoft.wake.remote.impl.ProxyEventHandler;
import com.microsoft.wake.remote.impl.RemoteEvent;
import com.microsoft.wake.remote.impl.RemoteReceiverStage;
import com.microsoft.wake.remote.impl.RemoteSenderStage;
import com.microsoft.wake.remote.impl.RemoteSeqNumGenerator;
import com.microsoft.wake.remote.transport.Transport;
import com.microsoft.wake.remote.transport.netty.NettyMessagingTransport;
import com.microsoft.wake.test.util.Monitor;
import com.microsoft.wake.test.util.TimeoutHandler;

public class SmallMessagesTest {

  @Rule public final TestName name = new TestName();

  final String logPrefix = "TEST ";

  @Test
  public void testRemoteTest() throws Exception {
    System.out.println(logPrefix + name.getMethodName());
    LoggingUtils.setLoggingLevel(Level.FINEST);

    Monitor monitor = new Monitor();
    TimerStage timer = new TimerStage(new TimeoutHandler(monitor), 60000, 60000);

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
    //int finalSize = 200000; // 6 events will be sent
    Map<Class<?>, EventHandler<?>> clazzToHandlerMap = new HashMap<Class<?>, EventHandler<?>>();
    Set<Object> set = Collections.synchronizedSet(new HashSet<Object>());
    clazzToHandlerMap.put(TestEvent.class, new ConsoleEventHandler<TestEvent>("recvEH1", set, finalSize, monitor));
    clazzToHandlerMap.put(TestEvent2.class, new ConsoleEventHandler<TestEvent2>("recvEH2", set, finalSize, monitor));
    EventHandler<Object> handler = new MultiEventHandler<Object>(clazzToHandlerMap);

    // receiver stage
    final RemoteReceiverStage reRecvStage = new RemoteReceiverStage(
        new RemoteEventHandler(decoder, handler), null, 10);

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


    // proxy handler for a remotely running handler
    ProxyEventHandler<TestEvent> proxyHandler1 = new ProxyEventHandler<TestEvent>(
        myId, remoteId, "recvEH1", reSendStage.<TestEvent>getHandler(), new RemoteSeqNumGenerator());
    long start = System.nanoTime();
    for (int i=0; i<finalSize; i++) {
      proxyHandler1.onNext(new TestEvent("0", i));
    }

    monitor.mwait();
    long end = System.nanoTime();
    long runtime_ns = end-start;
    double runtime_s = ((double)runtime_ns)/(1000*1000*1000);
    System.out.println("msgs/s: "+finalSize/runtime_s);// most time is spent in netty.channel.socket.nio.SelectorUtil.select()

    if (set.size() != finalSize) {
      Assert.fail(name.getMethodName() +
          " takes too long and times out : " + set.size() + " out of " + finalSize + " events");
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
    public void onNext(T event) {
      //System.out.println(name + " " + event);
      set.add(event);
      if (set.size() == finalSize) {
        monitor.mnotify();
      }
    }
  }
}
