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
package com.microsoft.reef.services.network.group;

import com.microsoft.reef.exception.evaluator.NetworkException;
import com.microsoft.reef.io.network.Connection;
import com.microsoft.reef.io.network.Message;
import com.microsoft.reef.io.network.group.impl.GCMCodec;
import com.microsoft.reef.io.network.group.impl.GroupCommNetworkHandler;
import com.microsoft.reef.io.network.group.impl.operators.ReceiverHelper;
import com.microsoft.reef.io.network.group.impl.operators.ReceiverHelperImpl;
import com.microsoft.reef.io.network.impl.MessagingTransportFactory;
import com.microsoft.reef.io.network.impl.NSMessage;
import com.microsoft.reef.io.network.impl.NetworkService;
import com.microsoft.reef.io.network.naming.NameServer;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type;
import com.microsoft.reef.io.network.util.ListCodec;
import com.microsoft.reef.io.network.util.StringCodec;
import com.microsoft.reef.io.network.util.StringIdentifierFactory;
import com.microsoft.reef.io.network.util.Utils;
import com.microsoft.reef.services.network.util.TestUtils;
import com.microsoft.wake.ComparableIdentifier;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.Identifier;
import com.microsoft.wake.remote.NetUtils;
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
  private static final NameServer nameService = new NameServer(0, idFac);
  private static final int nameServicePort = nameService.getPort();
  private static final List<Integer> nsPorts = new ArrayList<>(numTasks);
  private static final List<GroupCommNetworkHandler> gcnhs = new ArrayList<>(numTasks);
  private static List<NetworkService<GroupCommMessage>> netServices = new ArrayList<>(numTasks);

  /**
   * @throws java.lang.Exception
   */
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
      NetworkService<GroupCommMessage> netService = new NetworkService<>(
          idFac, 0, nameServiceAddr, nameServicePort, new GCMCodec(),
          new MessagingTransportFactory(), gcnh, exHandler);
      netService.registerId(id);
      netServices.add(netService);
      int port = netService.getTransport().getListeningPort();
      nameService.register(id, new InetSocketAddress(NetUtils.getLocalAddress(), port));
      nsPorts.add(port);
    }
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    for (NetworkService<GroupCommMessage> netService : netServices)
      netService.close();
    nameService.close();
  }

  /**
   * Test method for {@link com.microsoft.reef.io.network.group.impl.operators.ReceiverHelperImpl#ReceiverHelperImpl(com.microsoft.reef.io.network.impl.NetworkService, com.microsoft.wake.remote.Codec, com.microsoft.reef.io.network.group.impl.GroupCommNetworkHandler)}.
   */
  @Test
  public final void testReceiverHelperImpl() {
    ReceiverHelper<GroupCommMessage> receiver = new ReceiverHelperImpl<>(netServices.get(0), new GCMCodec(), gcnhs.get(0));
    Assert.assertNotNull("new ReceiverHelperImpl()", receiver);
  }

  /**
   * Test method for {@link com.microsoft.reef.io.network.group.impl.operators.ReceiverHelperImpl#receive(com.microsoft.wake.Identifier, com.microsoft.wake.Identifier, com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type)}.
   *
   * @throws InterruptedException
   * @throws NetworkException
   */
  @Test
  public final void testReceiveIdentifierIdentifierType() throws InterruptedException, NetworkException {
    StringCodec strCodec = new StringCodec();
    for (int i = 0; i < ids.size(); i++) {
      Identifier from = ids.get(i);
      NetworkService<GroupCommMessage> fromNetService = netServices.get(i);
      for (int j = 0; j < ids.size(); j++) {
        Identifier to = ids.get(j);
        NetworkService<GroupCommMessage> toNetService = netServices.get(j);
        GroupCommNetworkHandler gcnh = gcnhs.get(j);
        ReceiverHelper<String> receiver = new ReceiverHelperImpl<String>(toNetService, strCodec, gcnh);
        for (Type type : Type.values()) {
          if (TestUtils.controlMessage(type)) continue;
          String expected = "Hello" + i + j + type;
          byte[] expBytes = expected.getBytes();
          GroupCommMessage gcm = TestUtils.bldGCM(type, from, to,
              expBytes);
          sendMsg(fromNetService, to, gcm);
          String actual = receiver.receive(from, to, type);
          Assert.assertEquals("ReceiverHelper.receive(from,to,T): ", expected,
              actual);
        }
      }
    }
  }

  private void sendMsg(NetworkService<GroupCommMessage> fromNetService,
                       Identifier to, GroupCommMessage gcm) throws NetworkException {
    Connection<GroupCommMessage> conn = fromNetService.newConnection(to);
    conn.open();
//    LOG.log(Level.FINEST, "Connection Open");
    conn.write(gcm);
//    LOG.log(Level.FINEST, "Write call completed");
//    conn.close();
//    LOG.log(Level.FINEST, "Connection Closed");
  }

  /**
   * Test method for {@link com.microsoft.reef.io.network.group.impl.operators.ReceiverHelperImpl#receiveList(com.microsoft.wake.Identifier, com.microsoft.wake.Identifier, com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type)}.
   *
   * @throws NetworkException
   * @throws InterruptedException
   */
  @Test
  public final void testReceiveList() throws NetworkException, InterruptedException {
    StringCodec strCodec = new StringCodec();
    for (int i = 0; i < ids.size(); i++) {
      Identifier from = ids.get(i);
      NetworkService<GroupCommMessage> fromNetService = netServices.get(i);
      for (int j = 0; j < ids.size(); j++) {
        Identifier to = ids.get(j);
        NetworkService<GroupCommMessage> toNetService = netServices.get(j);
        GroupCommNetworkHandler gcnh = gcnhs.get(j);
        ReceiverHelper<String> receiver = new ReceiverHelperImpl<String>(toNetService, strCodec, gcnh);
        for (Type type : Type.values()) {
          if (TestUtils.controlMessage(type)) continue;
          List<String> expected = new ArrayList<>();
          byte[][] expBytes = new byte[5][];
          for (int k = 0; k < 5; k++) {
            String msg = "Hello" + i + j + type + k;
            expected.add(msg);
            byte[] msgBytes = msg.getBytes();
            expBytes[k] = msgBytes;
          }
          GroupCommMessage gcm = TestUtils.bldGCM(type, from, to,
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
   * Test method for {@link com.microsoft.reef.io.network.group.impl.operators.ReceiverHelperImpl#receiveListOfList(com.microsoft.wake.Identifier, com.microsoft.wake.Identifier, com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type)}.
   *
   * @throws NetworkException
   * @throws InterruptedException
   */
  @Test
  public final void testReceiveListOfList() throws NetworkException, InterruptedException {
    StringCodec strCodec = new StringCodec();
    ListCodec<String> lstCodec = new ListCodec<>(strCodec);
    for (int i = 0; i < ids.size(); i++) {
      Identifier from = ids.get(i);
      NetworkService<GroupCommMessage> fromNetService = netServices.get(i);
      for (int j = 0; j < ids.size(); j++) {
        Identifier to = ids.get(j);
        NetworkService<GroupCommMessage> toNetService = netServices.get(j);
        GroupCommNetworkHandler gcnh = gcnhs.get(j);
        ReceiverHelper<String> receiver = new ReceiverHelperImpl<String>(toNetService, strCodec, gcnh);
        for (Type type : Type.values()) {
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
          GroupCommMessage gcm = TestUtils.bldGCM(type, from, to,
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
   * Test method for {@link com.microsoft.reef.io.network.group.impl.operators.ReceiverHelperImpl#receive(java.util.List, com.microsoft.wake.Identifier, com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type)}.
   *
   * @throws NetworkException
   * @throws InterruptedException
   */
  @Test
  public final void testReceiveListOfQextendsIdentifierIdentifierType() throws NetworkException, InterruptedException {
    StringCodec strCodec = new StringCodec();

    List<Identifier> froms = new ArrayList<>(numTasks);
    List<NetworkService<GroupCommMessage>> netSers = new ArrayList<>(numTasks);
    int[] indexArr = {1, 3, 2, 4, 0};
    for (int i : indexArr) {
      froms.add(ids.get(i));
      netSers.add(netServices.get(i));
    }
    for (int j = 0; j < ids.size(); j++) {
      Identifier to = ids.get(j);
      NetworkService<GroupCommMessage> toNetService = netServices.get(j);
      GroupCommNetworkHandler gcnh = gcnhs.get(j);
      ReceiverHelper<String> receiver = new ReceiverHelperImpl<String>(toNetService, strCodec, gcnh);
      for (Type type : Type.values()) {
        if (TestUtils.controlMessage(type)) continue;
        List<String> expected = new ArrayList<>(froms.size());
        for (int k = 0; k < froms.size(); k++) {
          String msg = "Hello" + j + type + k;
          expected.add(msg);
          byte[] msgBytes = msg.getBytes();
          GroupCommMessage gcm = TestUtils.bldGCM(type, froms.get(k), to, msgBytes);
          sendMsg(netSers.get(k), to, gcm);
        }

        List<String> actual = receiver.receive(froms, to, type);
        Assert.assertEquals("ReceiverHelper.receive(List<from>,to,T): ", expected,
            actual);
      }
    }
  }

  /**
   * @param size
   * @param numMsgs
   * @param r
   * @return
   */
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

  /* (non-Javadoc)
   * @see com.microsoft.wake.EventHandler#send(java.lang.Object)
   */
  @Override
  public void onNext(Exception exc) {
    GroupCommMessage gcm = TestUtils.bldGCM(Type.Scatter, self, self, exc.getMessage().getBytes());
    Message<GroupCommMessage> msg = new NSMessage<GroupCommMessage>(self, self, gcm);
    gcnh.onNext(msg);
  }
}