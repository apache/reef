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
import com.microsoft.reef.io.network.Message;
import com.microsoft.reef.io.network.group.impl.GCMCodec;
import com.microsoft.reef.io.network.group.impl.operators.SenderHelper;
import com.microsoft.reef.io.network.group.impl.operators.SenderHelperImpl;
import com.microsoft.reef.io.network.impl.MessagingTransportFactory;
import com.microsoft.reef.io.network.impl.NetworkService;
import com.microsoft.reef.io.network.naming.NameServer;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type;
import com.microsoft.reef.io.network.util.ListCodec;
import com.microsoft.reef.io.network.util.StringCodec;
import com.microsoft.reef.io.network.util.StringIdentifierFactory;
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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class SenderTest {

  private static final StringIdentifierFactory idFac = new StringIdentifierFactory();
  private static final int numTasks = 5;
  private static final List<ComparableIdentifier> ids = new ArrayList<>(numTasks);
  private static final String nameServiceAddr = NetUtils.getLocalAddress();
  private static final NameServer nameService = new NameServer(0, idFac);
  private static final int nameServicePort = nameService.getPort();
  private static final List<Integer> nsPorts = new ArrayList<>(numTasks);
  private static final List<BlockingQueue<GroupCommMessage>> queues = new ArrayList<>(numTasks);
  private static List<NetworkService<GroupCommMessage>> netServices = new ArrayList<>(numTasks);

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    for (int i = 0; i < numTasks; i++) {
      Identifier id = idFac.getNewInstance("Task" + i);
      ids.add((ComparableIdentifier) id);
      queues.add(new LinkedBlockingQueue<GroupCommMessage>(5));
    }

    for (int i = 0; i < numTasks; i++) {
      Identifier id = ids.get(i);
      BlockingQueue<GroupCommMessage> queue = queues.get(i);
      EventHandler<Message<GroupCommMessage>> recvHandler = new RcvHandler(id, queue);
      EventHandler<Exception> exHandler = new SndExcHandler(id, queue);
      NetworkService<GroupCommMessage> netService = new NetworkService<>(
          idFac, 0, nameServiceAddr, nameServicePort, new GCMCodec(),
          new MessagingTransportFactory(), recvHandler, exHandler);
      netService.registerId(id);
      netServices.add(netService);
      int port = netService.getTransport().getListeningPort();
      nameService.register(id, new InetSocketAddress(NetUtils.getLocalAddress(), port));
      nsPorts.add(port);
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    for (NetworkService<GroupCommMessage> netService : netServices)
      netService.close();
    nameService.close();
  }

  /**
   * Test method for {@link com.microsoft.reef.io.network.group.impl.operators.SenderHelperImpl#SenderHelperImpl(com.microsoft.reef.io.network.impl.NetworkService, com.microsoft.wake.remote.Codec)}.
   */
  @Test
  public final void testSenderHelperImpl() {
    SenderHelper<GroupCommMessage> sender = new SenderHelperImpl<>(netServices.get(0), new GCMCodec());
    Assert.assertNotNull("new SenderHelperImp()", sender);
  }

  /**
   * Test method for {@link com.microsoft.reef.io.network.group.impl.operators.SenderHelperImpl#send(com.microsoft.wake.Identifier, com.microsoft.wake.Identifier, java.lang.Object, com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type)}.
   */
  @Test
  public final void testSendIdentifierIdentifierTType() throws NetworkException, InterruptedException {
    StringCodec strCodec = new StringCodec();
    for (int i = 0; i < ids.size(); i++) {
      Identifier from = ids.get(i);
      NetworkService<GroupCommMessage> fromNetService = netServices.get(i);
      SenderHelper<String> sender = new SenderHelperImpl<>(fromNetService,
          strCodec);
      for (int j = 0; j < ids.size(); j++) {
        Identifier to = ids.get(j);
        BlockingQueue<GroupCommMessage> toQue = queues.get(j);

        for (Type type : Type.values()) {
          if (TestUtils.controlMessage(type)) continue;
          String msg = "Hello" + i + j + type;
          byte[] msgBytes = msg.getBytes();
          GroupCommMessage expected = TestUtils.bldGCM(type, from, to,
              msgBytes);
          sender.send(from, to, msg, type);
          GroupCommMessage actual = toQue.poll(100, TimeUnit.MILLISECONDS);
          Assert.assertEquals("SenderHelper.send(from,to,T): ", expected,
              actual);
        }
      }
    }
  }

  /**
   * Test method for {@link com.microsoft.reef.io.network.group.impl.operators.SenderHelperImpl#send(com.microsoft.wake.Identifier, com.microsoft.wake.Identifier, java.util.List, com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type)}.
   */
  @Test
  public final void testSendIdentifierIdentifierListOfTType() throws NetworkException, InterruptedException {
    StringCodec strCodec = new StringCodec();
    for (int i = 0; i < ids.size(); i++) {
      Identifier from = ids.get(i);
      NetworkService<GroupCommMessage> fromNetService = netServices.get(i);
      SenderHelper<String> sender = new SenderHelperImpl<>(fromNetService,
          strCodec);
      for (int j = 0; j < ids.size(); j++) {
        Identifier to = ids.get(j);
        BlockingQueue<GroupCommMessage> toQue = queues.get(j);

        for (Type type : Type.values()) {
          if (TestUtils.controlMessage(type)) continue;
          List<String> msgs = new ArrayList<>();
          byte[][] msgsBytes = new byte[5][];
          for (int k = 0; k < 5; k++) {
            String msg = "Hello" + i + j + type + k;
            msgs.add(msg);
            byte[] msgBytes = msg.getBytes();
            msgsBytes[k] = msgBytes;
          }

          GroupCommMessage expected = TestUtils.bldGCM(type, from, to,
              msgsBytes);
          sender.send(from, to, msgs, type);
          GroupCommMessage actual = toQue.poll(100, TimeUnit.MILLISECONDS);
          Assert.assertEquals("SenderHelper.send(from,to,List<T>): ", expected,
              actual);
        }
      }
    }
  }

  /**
   * Test method for {@link com.microsoft.reef.io.network.group.impl.operators.SenderHelperImpl#sendListOfList(com.microsoft.wake.Identifier, com.microsoft.wake.Identifier, java.util.List, com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type)}.
   */
  @Test
  public final void testSendListOfList() throws NetworkException, InterruptedException {
    StringCodec strCodec = new StringCodec();
    ListCodec<String> lstCodec = new ListCodec<>(strCodec);
    for (int i = 0; i < ids.size(); i++) {
      Identifier from = ids.get(i);
      NetworkService<GroupCommMessage> fromNetService = netServices.get(i);
      SenderHelper<String> sender = new SenderHelperImpl<>(fromNetService,
          strCodec);
      for (int j = 0; j < ids.size(); j++) {
        Identifier to = ids.get(j);
        BlockingQueue<GroupCommMessage> toQue = queues.get(j);

        for (Type type : Type.values()) {
          if (TestUtils.controlMessage(type)) continue;
          List<List<String>> msgss = new ArrayList<>(5);
          byte[][] msgsBytes = new byte[5][];
          for (int l = 0; l < 5; l++) {
            List<String> msgs = new ArrayList<>();
            for (int k = 0; k < 5; k++)
              msgs.add("Hello" + i + j + type + l + k);
            msgss.add(msgs);
            msgsBytes[l] = lstCodec.encode(msgs);
          }
          GroupCommMessage expected = TestUtils.bldGCM(type, from, to,
              msgsBytes);
          sender.sendListOfList(from, to, msgss, type);
          GroupCommMessage actual = toQue.poll(100, TimeUnit.MILLISECONDS);
          Assert.assertEquals("SenderHelper.send(from,to,List<T>): ", expected,
              actual);
        }
      }
    }
  }

  /**
   * Test method for {@link com.microsoft.reef.io.network.group.impl.operators.SenderHelperImpl#send(com.microsoft.wake.Identifier, java.util.List, java.util.List, java.util.List, com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type)}.
   */
  @Test
  public final void testSendIdentifierListOfQextendsIdentifierListOfTListOfIntegerType() throws NetworkException, InterruptedException {
    Random r = new Random(1331);
    final StringCodec strCodec = new StringCodec();
    for (int i = 0; i < ids.size(); i++) {
      Identifier from = ids.get(i);
      NetworkService<GroupCommMessage> fromNetService = netServices.get(i);
      SenderHelper<String> sender = new SenderHelperImpl<>(fromNetService,
          strCodec);
      List<Identifier> tos = new ArrayList<>(numTasks - 1);
      int[] indexArr = {1, 3, 2, 4, 0};
      for (int j : indexArr) {
        tos.add(ids.get(j));
      }

      for (Type type : Type.values()) {
        if (TestUtils.controlMessage(type)) continue;
        final int numMsgs = r.nextInt(100) + 1;
        List<Integer> counts = genRndCounts(ids.size(), numMsgs, r);
        List<String> msgs = new ArrayList<>(numMsgs);
        GroupCommMessage[] expecteds = new GroupCommMessage[ids.size()];
        int cntr = 0;
        for (int c = 0; c < counts.size(); c++) {
          int count = counts.get(c);
          byte[][] msgsBytes = new byte[count][];
          for (int k = 0; k < count; k++) {
            String msg = "Hello" + i + type + (cntr++);
            msgs.add(msg);
            byte[] msgBytes = msg.getBytes();
            msgsBytes[k] = msgBytes;
          }
          expecteds[indexArr[c]] = TestUtils.bldGCM(type, from, tos.get(c), msgsBytes);
        }

        sender.send(from, tos, msgs, counts, type);
        GroupCommMessage[] actuals = new GroupCommMessage[ids.size()];
        for (int q : indexArr)
          actuals[q] = queues.get(q).poll(100, TimeUnit.MILLISECONDS);
        Assert.assertArrayEquals("SenderHelper.send(from,List<to>,List<T>,cnts): ", expecteds,
            actuals);
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

class RcvHandler implements EventHandler<Message<GroupCommMessage>> {

  Identifier self;
  BlockingQueue<GroupCommMessage> queue;

  public RcvHandler(Identifier self, BlockingQueue<GroupCommMessage> queue) {
    this.self = self;
    this.queue = queue;
  }

  @Override
  public void onNext(Message<GroupCommMessage> msg) {
    GroupCommMessage gcm = msg.getData().iterator().next();
    queue.add(gcm);
  }
}

class SndExcHandler implements EventHandler<Exception> {

  Identifier self;
  BlockingQueue<GroupCommMessage> queue;

  public SndExcHandler(Identifier self, BlockingQueue<GroupCommMessage> queue) {
    this.self = self;
    this.queue = queue;
  }

  @Override
  public void onNext(Exception exc) {
    GroupCommMessage gcm = TestUtils.bldGCM(Type.Scatter, self, self, exc.getMessage().getBytes());
    queue.add(gcm);
  }
}
