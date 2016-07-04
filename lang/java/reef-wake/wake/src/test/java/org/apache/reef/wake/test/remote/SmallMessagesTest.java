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

import java.util.*;
import java.util.logging.Level;

/**
 * Test transferring small messages.
 */
public class SmallMessagesTest {
  private final LocalAddressProvider localAddressProvider;
  private final TransportFactory tpFactory;

  public SmallMessagesTest() throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector();
    this.localAddressProvider = injector.getInstance(LocalAddressProvider.class);
    this.tpFactory = injector.getInstance(TransportFactory.class);
  }

  @Rule
  public final TestName name = new TestName();

  private static final String LOG_PREFIX = "TEST ";

  @Test
  public void testRemoteTest() throws Exception {
    System.out.println(LOG_PREFIX + name.getMethodName());
    LoggingUtils.setLoggingLevel(Level.FINEST);

    final Monitor monitor = new Monitor();
    final TimerStage timer = new TimerStage(new TimeoutHandler(monitor), 60000, 60000);

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
        new RemoteEventHandler(decoder, handler), null, 10);

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


    // proxy handler for a remotely running handler
    final ProxyEventHandler<TestEvent> proxyHandler1 = new ProxyEventHandler<>(
        myId, remoteId, "recvEH1", reSendStage.<TestEvent>getHandler(), new RemoteSeqNumGenerator());
    final long start = System.nanoTime();
    for (int i = 0; i < finalSize; i++) {
      proxyHandler1.onNext(new TestEvent("0", i));
    }

    monitor.mwait();
    final long end = System.nanoTime();
    final long runtimeNs = end - start;
    final double runtimeS = ((double) runtimeNs) / (1000 * 1000 * 1000);
    System.out.println("msgs/s: " + finalSize / runtimeS);
    // most time is spent in netty.channel.socket.nio.SelectorUtil.select()

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
    public void onNext(final T event) {
      //System.out.println(name + " " + event);
      set.add(event);
      if (set.size() == finalSize) {
        monitor.mnotify();
      }
    }
  }
}
