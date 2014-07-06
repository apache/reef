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

import java.net.InetSocketAddress;
import java.util.logging.Level;

import org.junit.Assert;

import org.junit.Test;

import com.microsoft.wake.EStage;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.impl.LoggingEventHandler;
import com.microsoft.wake.impl.LoggingUtils;
import com.microsoft.wake.impl.ThreadPoolStage;
import com.microsoft.wake.impl.TimerStage;
import com.microsoft.wake.remote.NetUtils;
import com.microsoft.wake.remote.impl.TransportEvent;
import com.microsoft.wake.remote.transport.Link;
import com.microsoft.wake.remote.transport.netty.NettyMessagingTransport;
import com.microsoft.wake.test.util.Monitor;
import com.microsoft.wake.test.util.PassThroughEncoder;
import com.microsoft.wake.test.util.TimeoutHandler;


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
