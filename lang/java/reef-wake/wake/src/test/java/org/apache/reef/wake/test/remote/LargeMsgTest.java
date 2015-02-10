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

import org.apache.reef.wake.EStage;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.impl.LoggingEventHandler;
import org.apache.reef.wake.impl.LoggingUtils;
import org.apache.reef.wake.impl.ThreadPoolStage;
import org.apache.reef.wake.impl.TimerStage;
import org.apache.reef.wake.remote.NetUtils;
import org.apache.reef.wake.remote.impl.TransportEvent;
import org.apache.reef.wake.remote.transport.Link;
import org.apache.reef.wake.remote.transport.netty.NettyMessagingTransport;
import org.apache.reef.wake.test.util.Monitor;
import org.apache.reef.wake.test.util.PassThroughEncoder;
import org.apache.reef.wake.test.util.TimeoutHandler;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.logging.Level;

/**
 * Test transferring large messages
 */
public class LargeMsgTest {
  private final static byte[][] values = new byte[3][];
  private final static int l0 = 1 << 25;
  private final static int l1 = 1 << 2;
  private final static int l2 = 1 << 21;

  @BeforeClass
  public static void setUpBeforeClass() {
    values[0] = new byte[l0];
    for (int i = 1; i < 25; i += 3) {
      values[0][1 << i] = (byte) i;
    }

    values[1] = new byte[l1];
    values[1][0] = (byte) 5;
    values[1][3] = (byte) 94;

    values[2] = new byte[l2];
    for (int i = 1; i < 21; i += 4) {
      values[2][1 << i] = (byte) (i * 2);
    }
  }

  @Test
  public void testLargeWrite() throws Exception {
    LoggingUtils.setLoggingLevel(Level.FINE);
    Monitor monitor = new Monitor();
    TimerStage timer = new TimerStage(new TimeoutHandler(monitor), 20000, 20000);

    long dataSize = values[0].length + values[1].length + values[2].length;

    EStage<TransportEvent> clientStage = new ThreadPoolStage<>("client1",
        new LoggingEventHandler<TransportEvent>(), 1, new LoggingEventHandler<Throwable>());
    EStage<TransportEvent> serverStage = new ThreadPoolStage<>("server@7001",
        new ServerHandler(monitor, dataSize), 1, new LoggingEventHandler<Throwable>());

    String hostAddress = NetUtils.getLocalAddress();
    int port = 7001;
    NettyMessagingTransport transport = new NettyMessagingTransport(hostAddress, port, clientStage, serverStage, 1, 10000);
    final Link<byte[]> link = transport.open(new InetSocketAddress(hostAddress, port), new PassThroughEncoder(), null);
    EStage<byte[]> writeSubmitter = new ThreadPoolStage<>("Submitter", new EventHandler<byte[]>() {

      @Override
      public void onNext(byte[] value) {
        try {
          link.write(value);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }, 3, new LoggingEventHandler<Throwable>());
    writeSubmitter.onNext(values[0]);
    writeSubmitter.onNext(values[1]);
    writeSubmitter.onNext(values[2]);

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

    ServerHandler(Monitor monitor, long expectedSize) {
      this.monitor = monitor;
      this.expectedSize = expectedSize;
      this.accSize = 0;
    }

    @Override
    public void onNext(TransportEvent value) {
      byte[] data = value.getData();

      switch (data.length) {
        case l0:
          Assert.assertArrayEquals(values[0], data);
          break;
        case l1:
          Assert.assertArrayEquals(values[1], data);
          break;
        case l2:
          Assert.assertArrayEquals(values[2], data);
          break;
      }
      accSize += data.length;
      if (accSize == expectedSize)
        monitor.mnotify();
    }

  }
}
