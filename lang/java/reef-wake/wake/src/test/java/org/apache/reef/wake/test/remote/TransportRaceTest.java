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
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.logging.Level;


public class TransportRaceTest {

  @Test
  public void testRace() throws Exception {
    LoggingUtils.setLoggingLevel(Level.FINE);
    Monitor monitor = new Monitor();
    TimerStage timer = new TimerStage(new TimeoutHandler(monitor), 5000, 5000);
    EStage<TransportEvent> clientStage = new ThreadPoolStage<>("client1",
        new LoggingEventHandler<TransportEvent>(), 1, new LoggingEventHandler<Throwable>());

    int msgsSent = 100;
    final ServerHandler serverHandler = new ServerHandler(monitor, msgsSent);
    EStage<TransportEvent> serverStage = new ThreadPoolStage<>("server@7001",
        serverHandler, 1, new LoggingEventHandler<Throwable>());
    String hostAddress = NetUtils.getLocalAddress();
    int port = 7001;
    NettyMessagingTransport transport = new NettyMessagingTransport(
        hostAddress, port, clientStage, serverStage, 1, 10000);

    String value = "Test Race";

    for (int i = 0; i < msgsSent; i++) {
      final Link<byte[]> link = transport.open(new InetSocketAddress(
          hostAddress, port), new PassThroughEncoder(), null);
      link.write(value.getBytes());
    }

    monitor.mwait();
    int msgsRcvd = serverHandler.getAccSize();
    if (msgsRcvd != msgsSent)
      Assert.assertEquals("Num Msgs transmitted==Num Msgs received", msgsSent,
          msgsRcvd);
    transport.close();
    clientStage.close();
    serverStage.close();
    timer.close();
  }

  class ServerHandler implements EventHandler<TransportEvent> {

    private final Monitor monitor;
    private final int expectedSize;
    private int accSize;

    ServerHandler(Monitor monitor, int expectedSize) {
      this.monitor = monitor;
      this.expectedSize = expectedSize;
      this.accSize = 0;
    }

    public int getAccSize() {
      return accSize;
    }

    @Override
    public void onNext(TransportEvent value) {
      ++accSize;
      if (accSize == expectedSize)
        monitor.mnotify();
    }

  }

}
