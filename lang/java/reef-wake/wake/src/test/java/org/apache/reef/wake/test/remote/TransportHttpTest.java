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
import org.apache.reef.wake.EStage;
import org.apache.reef.wake.impl.LoggingUtils;
import org.apache.reef.wake.impl.TimerStage;
import org.apache.reef.wake.remote.Codec;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.remote.impl.ObjectSerializableCodec;
import org.apache.reef.wake.remote.impl.TransportEvent;
import org.apache.reef.wake.remote.transport.Link;
import org.apache.reef.wake.remote.transport.Transport;
import org.apache.reef.wake.remote.transport.TransportFactory;
import org.apache.reef.wake.remote.transport.netty.MessagingTransportFactory;
import org.apache.reef.wake.remote.transport.netty.LoggingLinkListener;
import org.apache.reef.wake.test.util.Monitor;
import org.apache.reef.wake.test.util.TimeoutHandler;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import static org.apache.reef.wake.remote.RemoteConfiguration.PROTOCOL_HTTP;


/**
 * Tests for Transport via http.
 */
public class TransportHttpTest {
  private final LocalAddressProvider localAddressProvider;
  private final TransportFactory tpFactory;

  public TransportHttpTest() throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector();
    this.localAddressProvider = injector.getInstance(LocalAddressProvider.class);
    this.tpFactory = injector.getInstance(MessagingTransportFactory.class);
  }

  private static final String LOG_PREFIX = "TEST ";
  @Rule
  public TestName name = new TestName();

  @Test
  public void testHttpTransportString() throws Exception {
    System.out.println(LOG_PREFIX + name.getMethodName());
    LoggingUtils.setLoggingLevel(Level.INFO);

    final Monitor monitor = new Monitor();
    final TimerStage timer = new TimerStage(new TimeoutHandler(monitor), 2000, 2000);

    final int expected = 2;

    final String hostAddress = localAddressProvider.getLocalAddress();

    // Codec<String>
    final ReceiverStage<String> stage =
        new ReceiverStage<>(new ObjectSerializableCodec<String>(), monitor, expected);
    final Transport transport = tpFactory.newInstance(hostAddress, 0, stage, stage, 1, 10000, PROTOCOL_HTTP);
    final int port = transport.getListeningPort();

    // sending side
    final Link<String> link = transport.open(
        new InetSocketAddress(hostAddress, port),
        new ObjectSerializableCodec<String>(),
        new LoggingLinkListener<String>());
    link.write(new String("hello1"));
    link.write(new String("hello2"));

    monitor.mwait();
    transport.close();
    timer.close();

    Assert.assertEquals(expected, stage.getCount());
  }

  @Test
  public void testHttpTransportTestEvent() throws Exception {
    System.out.println(LOG_PREFIX + name.getMethodName());
    LoggingUtils.setLoggingLevel(Level.INFO);

    final Monitor monitor = new Monitor();
    final TimerStage timer = new TimerStage(new TimeoutHandler(monitor), 2000, 2000);

    final int expected = 2;
    final String hostAddress = localAddressProvider.getLocalAddress();

    // Codec<TestEvent>
    final ReceiverStage<TestEvent> stage =
        new ReceiverStage<>(new ObjectSerializableCodec<TestEvent>(), monitor, expected);
    final Transport transport = tpFactory.newInstance(hostAddress, 0, stage, stage, 1, 10000, PROTOCOL_HTTP);
    final int port = transport.getListeningPort();

    // sending side
    final Link<TestEvent> link = transport.open(
        new InetSocketAddress(hostAddress, port),
        new ObjectSerializableCodec<TestEvent>(),
        new LoggingLinkListener<TestEvent>());
    link.write(new TestEvent("hello1", 0.0));
    link.write(new TestEvent("hello2", 1.0));

    monitor.mwait();
    transport.close();
    timer.close();

    Assert.assertEquals(expected, stage.getCount());
  }

  class ReceiverStage<T> implements EStage<TransportEvent> {

    private final Codec<T> codec;
    private final Monitor monitor;
    private final int expected;
    private AtomicInteger count = new AtomicInteger(0);

    ReceiverStage(final Codec<T> codec, final Monitor monitor, final int expected) {
      this.codec = codec;
      this.monitor = monitor;
      this.expected = expected;
    }

    int getCount() {
      return count.get();
    }

    @Override
    public void onNext(final TransportEvent value) {
      codec.decode(value.getData());

      if (count.incrementAndGet() == expected) {
        monitor.mnotify();
      }
    }

    @Override
    public void close() throws Exception {
    }

  }

}
