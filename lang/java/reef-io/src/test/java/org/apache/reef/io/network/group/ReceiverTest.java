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
package org.apache.reef.io.network.group;

import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.Connection;
import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.group.impl.GCMCodec;
import org.apache.reef.io.network.group.impl.GroupCommNetworkHandler;
import org.apache.reef.io.network.group.impl.operators.ReceiverHelper;
import org.apache.reef.io.network.group.impl.operators.ReceiverHelperImpl;
import org.apache.reef.io.network.impl.MessagingTransportFactory;
import org.apache.reef.io.network.impl.NSMessage;
import org.apache.reef.io.network.impl.NetworkService;
import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.io.network.naming.NameServerImpl;
import org.apache.reef.io.network.proto.ReefNetworkGroupCommProtos;
import org.apache.reef.io.network.util.*;
import org.apache.reef.wake.ComparableIdentifier;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.remote.NetUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class ReceiverTest {
  private static final StringIdentifierFactory idFac = new StringIdentifierFactory();
  private static final int numTasks = 5;
  private static final List<ComparableIdentifier> ids = new ArrayList<>(numTasks);
  private static final String nameServiceAddr = NetUtils.getLocalAddress();
  private static final NameServer nameService = new NameServerImpl(0, idFac);
  private static final int nameServicePort = nameService.getPort();
  private static final List<Integer> nsPorts = new ArrayList<>(numTasks);
  private static final List<GroupCommNetworkHandler> gcnhs = new ArrayList<>(numTasks);
  private static List<NetworkService<ReefNetworkGroupCommProtos.GroupCommMessage>> netServices = new ArrayList<>(numTasks);

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    for (int i = 0; i < numTasks; i++) {
      Identifier id = idFac.getNewInstance("Task" + i);

      ids.add((ComparableIdentifier) id);
    }

    for (int i = 0; i < numTasks; i++) {
      Identifier id = ids.get(i);
      GroupCommNetworkHandler gcnh = new GroupCommNetworkHandler(Utils.listToString(ids), idFac, 5);
      gcnhs.add(gcnh);
      EventHandler<Exception> exHandler = new RcvExcHandler(id, gcnh);
      NetworkService<ReefNetworkGroupCommProtos.GroupCommMessage> netService = new NetworkService<>(
          idFac, 0, nameServiceAddr, nameServicePort, new GCMCodec(),
          new MessagingTransportFactory(), gcnh, exHandler);
      netService.registerId(id);
      netServices.add(netService);
      int port = netService.getTransport().getListeningPort();
      nameService.register(id, new InetSocketAddress(NetUtils.getLocalAddress(), port));
      nsPorts.add(port);
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    for (NetworkService<ReefNetworkGroupCommProtos.GroupCommMessage> netService : netServices)
      netService.close();
    nameService.close();
  }

  /**
   * Test method for {@link org.apache.reef.io.network.group.impl.operators.ReceiverHelperImpl#ReceiverHelperImpl(org.apache.reef.io.network.impl.NetworkService, org.apache.reef.wake.remote.Codec, org.apache.reef.io.network.group.impl.GroupCommNetworkHandler)}.
   */
  @Test
  public final void testReceiverHelperImpl() {
    ReceiverHelper<ReefNetworkGroupCommProtos.GroupCommMessage> receiver = new ReceiverHelperImpl<>(netServices.get(0), new GCMCodec(), gcnhs.get(0));
    Assert.assertNotNull("new ReceiverHelperImpl()", receiver);
  }

  /**
   * Test method for {@link org.apache.reef.io.network.group.impl.operators.ReceiverHelperImpl#receive(org.apache.reef.wake.Identifier, org.apache.reef.wake.Identifier, org.apache.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type)}.
   */
  @Test
  public final void testReceiveIdentifierIdentifierType() throws InterruptedException, NetworkException {
    StringCodec strCodec = new StringCodec();
    for (int i = 0; i < ids.size(); i++) {
      Identifier from = ids.get(i);
      NetworkService<ReefNetworkGroupCommProtos.GroupCommMessage> fromNetService = netServices.get(i);
      for (int j = 0; j < ids.size(); j++) {
        Identifier to = ids.get(j);
        NetworkService<ReefNetworkGroupCommProtos.GroupCommMessage> toNetService = netServices.get(j);
        GroupCommNetworkHandler gcnh = gcnhs.get(j);
        ReceiverHelper<String> receiver = new ReceiverHelperImpl<String>(toNetService, strCodec, gcnh);
        for (ReefNetworkGroupCommProtos.GroupCommMessage.Type type : ReefNetworkGroupCommProtos.GroupCommMessage.Type.values()) {
          if (TestUtils.controlMessage(type)) continue;
          String expected = "Hello" + i + j + type;
          byte[] expBytes = expected.getBytes();
          ReefNetworkGroupCommProtos.GroupCommMessage gcm = TestUtils.bldGCM(type, from, to,
              expBytes);
          sendMsg(fromNetService, to, gcm);
          String actual = receiver.receive(from, to, type);
          Assert.assertEquals("ReceiverHelper.receive(from,to,T): ", expected,
              actual);
        }
      }
    }
  }

  private void sendMsg(NetworkService<ReefNetworkGroupCommProtos.GroupCommMessage> fromNetService,
                       Identifier to, ReefNetworkGroupCommProtos.GroupCommMessage gcm) throws NetworkException {
    Connection<ReefNetworkGroupCommProtos.GroupCommMessage> conn = fromNetService.newConnection(to);
    conn.open();
    conn.write(gcm);
  }

  /**
   * Test method for {@link org.apache.reef.io.network.group.impl.operators.ReceiverHelperImpl#receiveList(org.apache.reef.wake.Identifier, org.apache.reef.wake.Identifier, org.apache.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type)}.
   */
  @Test
  public final void testReceiveList() throws NetworkException, InterruptedException {
    StringCodec strCodec = new StringCodec();
    for (int i = 0; i < ids.size(); i++) {
      Identifier from = ids.get(i);
      NetworkService<ReefNetworkGroupCommProtos.GroupCommMessage> fromNetService = netServices.get(i);
      for (int j = 0; j < ids.size(); j++) {
        Identifier to = ids.get(j);
        NetworkService<ReefNetworkGroupCommProtos.GroupCommMessage> toNetService = netServices.get(j);
        GroupCommNetworkHandler gcnh = gcnhs.get(j);
        ReceiverHelper<String> receiver = new ReceiverHelperImpl<String>(toNetService, strCodec, gcnh);
        for (ReefNetworkGroupCommProtos.GroupCommMessage.Type type : ReefNetworkGroupCommProtos.GroupCommMessage.Type.values()) {
          if (TestUtils.controlMessage(type)) continue;
          List<String> expected = new ArrayList<>();
          byte[][] expBytes = new byte[5][];
          for (int k = 0; k < 5; k++) {
            String msg = "Hello" + i + j + type + k;
            expected.add(msg);
            byte[] msgBytes = msg.getBytes();
            expBytes[k] = msgBytes;
          }
          ReefNetworkGroupCommProtos.GroupCommMessage gcm = TestUtils.bldGCM(type, from, to,
              expBytes);
          sendMsg(fromNetService, to, gcm);
          List<String> actual = receiver.receiveList(from, to, type);
          Assert.assertEquals("receiver.receiveList(from,to,T): ", expected,
              actual);
        }
      }
    }
  }

  /**
   * Test method for {@link org.apache.reef.io.network.group.impl.operators.ReceiverHelperImpl#receiveListOfList(org.apache.reef.wake.Identifier, org.apache.reef.wake.Identifier, org.apache.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type)}.
   */
  @Test
  public final void testReceiveListOfList() throws NetworkException, InterruptedException {
    StringCodec strCodec = new StringCodec();
    ListCodec<String> lstCodec = new ListCodec<>(strCodec);
    for (int i = 0; i < ids.size(); i++) {
      Identifier from = ids.get(i);
      NetworkService<ReefNetworkGroupCommProtos.GroupCommMessage> fromNetService = netServices.get(i);
      for (int j = 0; j < ids.size(); j++) {
        Identifier to = ids.get(j);
        NetworkService<ReefNetworkGroupCommProtos.GroupCommMessage> toNetService = netServices.get(j);
        GroupCommNetworkHandler gcnh = gcnhs.get(j);
        ReceiverHelper<String> receiver = new ReceiverHelperImpl<String>(toNetService, strCodec, gcnh);
        for (ReefNetworkGroupCommProtos.GroupCommMessage.Type type : ReefNetworkGroupCommProtos.GroupCommMessage.Type.values()) {
          if (TestUtils.controlMessage(type)) continue;
          List<List<String>> expected = new ArrayList<>();
          byte[][] expBytes = new byte[5][];
          for (int l = 0; l < 5; l++) {
            List<String> msgs = new ArrayList<>();
            for (int k = 0; k < 5; k++)
              msgs.add("Hello" + i + j + type + l + k);
            expected.add(msgs);
            expBytes[l] = lstCodec.encode(msgs);
          }
          ReefNetworkGroupCommProtos.GroupCommMessage gcm = TestUtils.bldGCM(type, from, to,
              expBytes);
          sendMsg(fromNetService, to, gcm);
          List<List<String>> actual = receiver.receiveListOfList(from, to, type);
          Assert.assertEquals("receiver.receiveList(from,to,T): ", expected,
              actual);
        }
      }
    }
  }

  /**
   * Test method for {@link org.apache.reef.io.network.group.impl.operators.ReceiverHelperImpl#receive(java.util.List, org.apache.reef.wake.Identifier, org.apache.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type)}.
   */
  @Test
  public final void testReceiveListOfQextendsIdentifierIdentifierType() throws NetworkException, InterruptedException {
    StringCodec strCodec = new StringCodec();

    List<Identifier> froms = new ArrayList<>(numTasks);
    List<NetworkService<ReefNetworkGroupCommProtos.GroupCommMessage>> netSers = new ArrayList<>(numTasks);
    int[] indexArr = {1, 3, 2, 4, 0};
    for (int i : indexArr) {
      froms.add(ids.get(i));
      netSers.add(netServices.get(i));
    }
    for (int j = 0; j < ids.size(); j++) {
      Identifier to = ids.get(j);
      NetworkService<ReefNetworkGroupCommProtos.GroupCommMessage> toNetService = netServices.get(j);
      GroupCommNetworkHandler gcnh = gcnhs.get(j);
      ReceiverHelper<String> receiver = new ReceiverHelperImpl<String>(toNetService, strCodec, gcnh);
      for (ReefNetworkGroupCommProtos.GroupCommMessage.Type type : ReefNetworkGroupCommProtos.GroupCommMessage.Type.values()) {
        if (TestUtils.controlMessage(type)) continue;
        List<String> expected = new ArrayList<>(froms.size());
        for (int k = 0; k < froms.size(); k++) {
          String msg = "Hello" + j + type + k;
          expected.add(msg);
          byte[] msgBytes = msg.getBytes();
          ReefNetworkGroupCommProtos.GroupCommMessage gcm = TestUtils.bldGCM(type, froms.get(k), to, msgBytes);
          sendMsg(netSers.get(k), to, gcm);
        }

        List<String> actual = receiver.receive(froms, to, type);
        Assert.assertEquals("ReceiverHelper.receive(List<from>,to,T): ", expected,
            actual);
      }
    }
  }

  private List<Integer> genRndCounts(int size, int numMsgs, Random r) {
    List<Integer> retLst = new ArrayList<>(size);
    int total = size;
    int remSize = numMsgs;
    while (total > 1) {
      int curSize = r.nextInt(remSize);
      remSize -= curSize;
      retLst.add(curSize);
      --total;
    }
    retLst.add(remSize);
    return retLst;
  }
}

class RcvExcHandler implements EventHandler<Exception> {
  Identifier self;
  GroupCommNetworkHandler gcnh;

  public RcvExcHandler(Identifier self, GroupCommNetworkHandler gcnh) {
    this.self = self;
    this.gcnh = gcnh;
  }

  /**
   * @see org.apache.reef.wake.EventHandler#send(java.lang.Object)
   */
  @Override
  public void onNext(Exception exc) {
    ReefNetworkGroupCommProtos.GroupCommMessage gcm = TestUtils.bldGCM(ReefNetworkGroupCommProtos.GroupCommMessage.Type.Scatter, self, self, exc.getMessage().getBytes());
    Message<ReefNetworkGroupCommProtos.GroupCommMessage> msg = new NSMessage<ReefNetworkGroupCommProtos.GroupCommMessage>(self, self, gcm);
    gcnh.onNext(msg);
  }
}
