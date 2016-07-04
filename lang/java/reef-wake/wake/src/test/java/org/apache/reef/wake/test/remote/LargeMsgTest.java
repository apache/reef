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
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.impl.LoggingEventHandler;
import org.apache.reef.wake.impl.LoggingUtils;
import org.apache.reef.wake.impl.ThreadPoolStage;
import org.apache.reef.wake.impl.TimerStage;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.remote.impl.TransportEvent;
import org.apache.reef.wake.remote.transport.Link;
import org.apache.reef.wake.remote.transport.Transport;
import org.apache.reef.wake.remote.transport.TransportFactory;
import org.apache.reef.wake.test.util.Monitor;
import org.apache.reef.wake.test.util.PassThroughEncoder;
import org.apache.reef.wake.test.util.TimeoutHandler;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.logging.Level;

/**
 * Test transferring large messages.
 */
public class LargeMsgTest {
  private final LocalAddressProvider localAddressProvider;
  private final TransportFactory tpFactory;
  private static final byte[][] VALUES = new byte[3][];
  private static final int L_0 = 1 << 25;
  private static final int L_1 = 1 << 2;
  private static final int L_2 = 1 << 21;

  public LargeMsgTest() throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector();
    this.localAddressProvider = injector.getInstance(LocalAddressProvider.class);
    this.tpFactory = injector.getInstance(TransportFactory.class);
  }

  @BeforeClass
  public static void setUpBeforeClass() {
    VALUES[0] = new byte[L_0];
    for (int i = 1; i < 25; i += 3) {
      VALUES[0][1 << i] = (byte) i;
    }

    VALUES[1] = new byte[L_1];
    VALUES[1][0] = (byte) 5;
    VALUES[1][3] = (byte) 94;

    VALUES[2] = new byte[L_2];
    for (int i = 1; i < 21; i += 4) {
      VALUES[2][1 << i] = (byte) (i * 2);
    }
  }

  @Test
  public void testLargeWrite() throws Exception {
    LoggingUtils.setLoggingLevel(Level.FINE);
    final Monitor monitor = new Monitor();
    final TimerStage timer = new TimerStage(new TimeoutHandler(monitor), 20000, 20000);

    final long dataSize = VALUES[0].length + VALUES[1].length + VALUES[2].length;

    final EStage<TransportEvent> clientStage = new ThreadPoolStage<>("client1",
        new LoggingEventHandler<TransportEvent>(), 1, new LoggingEventHandler<Throwable>());
    final EStage<TransportEvent> serverStage = new ThreadPoolStage<>("server@7001",
        new ServerHandler(monitor, dataSize), 1, new LoggingEventHandler<Throwable>());

    final String hostAddress = this.localAddressProvider.getLocalAddress();
    final Transport transport = tpFactory.newInstance(hostAddress, 0, clientStage, serverStage, 1, 10000);
    final int port = transport.getListeningPort();
    final Link<byte[]> link = transport.open(new InetSocketAddress(hostAddress, port), new PassThroughEncoder(), null);
    final EStage<byte[]> writeSubmitter = new ThreadPoolStage<>("Submitter", new EventHandler<byte[]>() {

      @Override
      public void onNext(final byte[] value) {
        link.write(value);
      }
    }, 3, new LoggingEventHandler<Throwable>());
    writeSubmitter.onNext(VALUES[0]);
    writeSubmitter.onNext(VALUES[1]);
    writeSubmitter.onNext(VALUES[2]);

    monitor.mwait();

    transport.close();
    clientStage.close();
    serverStage.close();
    timer.close();
  }

  class ServerHandler implements EventHandler<TransportEvent> {

    private final Monitor monitor;
    private final long expectedSize;
    private long accSize;

    ServerHandler(final Monitor monitor, final long expectedSize) {
      this.monitor = monitor;
      this.expectedSize = expectedSize;
      this.accSize = 0;
    }

    @Override
    public void onNext(final TransportEvent value) {
      final byte[] data = value.getData();

      switch (data.length) {
      case L_0:
        Assert.assertArrayEquals(VALUES[0], data);
        break;
      case L_1:
        Assert.assertArrayEquals(VALUES[1], data);
        break;
      case L_2:
        Assert.assertArrayEquals(VALUES[2], data);
        break;
      default:
        break;
      }
      accSize += data.length;
      if (accSize == expectedSize) {
        monitor.mnotify();
      }
    }

  }
}
