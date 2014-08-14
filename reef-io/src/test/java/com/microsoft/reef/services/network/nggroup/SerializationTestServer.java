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
package com.microsoft.reef.services.network.nggroup;

import com.microsoft.reef.io.network.nggroup.impl.ResettingCDL;
import com.microsoft.reef.io.network.util.StringIdentifierFactory;
import com.microsoft.wake.EStage;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.impl.LoggingEventHandler;
import com.microsoft.wake.impl.ThreadPoolStage;
import com.microsoft.wake.impl.TimerStage;
import com.microsoft.wake.remote.NetUtils;
import com.microsoft.wake.remote.impl.ByteCodec;
import com.microsoft.wake.remote.impl.TransportEvent;
import com.microsoft.wake.remote.transport.Link;
import com.microsoft.wake.remote.transport.netty.NettyMessagingTransport;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.net.InetSocketAddress;
import java.util.List;

public class SerializationTestServer {

  public static void main(final String[] args) throws Exception {
//    LoggingUtils.setLoggingLevel(Level.FINEST);
    final Monitor monitor = new Monitor();
    final int numMsgs = 50;
    final TimerStage timer = new TimerStage(new TimeoutHandler(monitor), 20000, 20000);
    final ResettingCDL cdl = new ResettingCDL(1);
    final EStage<TransportEvent> clientStage = new ThreadPoolStage<>("client",
        new ClientHandler(cdl), 1, new LoggingEventHandler<Throwable>());
    final EStage<TransportEvent> serverStage = new ThreadPoolStage<>("server@7001",
        new ServerHandler(monitor, numMsgs), 1, new LoggingEventHandler<Throwable>());
    final String hostAddress = NetUtils.getLocalAddress();
    final int port = 7001;
    final NettyMessagingTransport transport = new NettyMessagingTransport(hostAddress, port, clientStage, serverStage, 1, 10000);
    final StringIdentifierFactory idFac = new StringIdentifierFactory();
//    final Link<NSMessage<GroupCommMessage>> link = transport.open(new InetSocketAddress(hostAddress, port),
//            new NSMessageCodec<>(new GCMCodec(), idFac), null);
    final Link<byte[]> link = transport.open(new InetSocketAddress(hostAddress, port),
        new ByteCodec(), null);
    printHeader();
    /*final byte[] msg = new byte[1 << 28];
    for (int i = 0; i < numMsgs; i++) {
//      GroupCommMessage msg;
      try (final MemoryDump md = new MemoryDump("Building GCM")) {
//        msg = Utils.bldVersionedGCM(AllCommunicationGroup.class, ControlMessageBroadcaster.class, Type.Broadcast,
//                "client", 0, "server@7001", 0, new byte[1 << 28]);
//        link.write(
//                new NSMessage<>(idFac.getNewInstance("client"),
//                        idFac.getNewInstance("server@7001"), msg));
        link.write(msg);
//        System.out.println("Wrote data and waiting for cdl");
        cdl.awaitAndReset(1);
//        System.out.println("CDL reset");
        System.gc();
      }
    }*/
    monitor.mwait();

    transport.close();
    clientStage.close();
    serverStage.close();
    timer.close();
  }

  private static void printHeader() {
    final List<MemoryPoolMXBean> memoryPoolMXBeans = ManagementFactory.getMemoryPoolMXBeans();
    String output = memoryPoolMXBeans.get(1).getName();
    for (int i = 2; i < memoryPoolMXBeans.size(); i++) {
      output += "," + memoryPoolMXBeans.get(i).getName();
    }
    System.out.println(output);
  }

  static class ClientHandler implements EventHandler<TransportEvent> {
    private final ResettingCDL cdl;

    public ClientHandler(final ResettingCDL cdl) {
      this.cdl = cdl;
    }

    @Override
    public void onNext(final TransportEvent arg0) {
//      System.out.println("Got ACK, counting down");
      cdl.countDown();
    }
  }

  static class ServerHandler implements EventHandler<TransportEvent> {

    private final Monitor monitor;
    private final int expEvents;
    private int actEvents = 0;

    ServerHandler(final Monitor monitor,
                  final int expEvents) {
      this.monitor = monitor;
      this.expEvents = expEvents;
    }

    @Override
    public void onNext(final TransportEvent value) {
//      final NSMessage<GroupCommMessage> nsMsg = new NSMessageCodec<>(new GCMCodec(), new StringIdentifierFactory()).decode(value.getData());
//      final GroupCommMessage gcm = nsMsg.getData().get(0);
//      final NSMessage<byte[]> nsMsg = new NSMessageCodec<>(new ByteCodec(), new StringIdentifierFactory()).decode(value.getData());
      try {
        try (final MemoryDump md = new MemoryDump("Building GCM")) {
          final byte[] gcm = value.getData();

          value.getLink().write(new byte[1]);
          //        System.out.println("Received data, wrote ACK");
        }
      } catch (final Exception e) {
        throw new RuntimeException("IOException", e);
      }
      ++actEvents;
      if (actEvents == expEvents) {
        monitor.mnotify();
      }
    }
  }

  static class MemoryDump implements AutoCloseable {

    private final String desc;

    public MemoryDump(final String desc) {
      this.desc = desc;
      dumpMemStats(false);
//    final String output = desc + " Started. Memory Usage:\n" + dumpMemStats(false);
//    System.out.println(output);
//    System.out.println("************************************************");
    }

    private void dumpMemStats(final boolean peak) {

      final List<MemoryPoolMXBean> memoryPoolMXBeans = ManagementFactory.getMemoryPoolMXBeans();
      String output = "" + (peak ? memoryPoolMXBeans.get(1).getPeakUsage() : memoryPoolMXBeans.get(1).getUsage()).getUsed();
      for (int i = 2; i < memoryPoolMXBeans.size(); i++) {
        output += "," + (peak ? memoryPoolMXBeans.get(i).getPeakUsage() : memoryPoolMXBeans.get(i).getUsage()).getUsed();
      }
      System.out.println(output);
    }

    private void resetPeakUsage() {
      final List<MemoryPoolMXBean> memoryPoolMXBeans = ManagementFactory.getMemoryPoolMXBeans();
      for (final MemoryPoolMXBean memoryPoolMXBean : memoryPoolMXBeans) {
        memoryPoolMXBean.resetPeakUsage();
      }
    }

    @Override
    public void close() throws Exception {
      dumpMemStats(true);
      resetPeakUsage();
//    final String output = desc + " Ended. Memory Usage:\n" + dumpMemStats(true);
//    System.out.println(output);
//    System.out.println("************************************************");
    }
  }
}
