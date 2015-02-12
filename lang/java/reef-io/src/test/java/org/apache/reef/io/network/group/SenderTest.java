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
import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.group.impl.GCMCodec;
import org.apache.reef.io.network.group.impl.operators.SenderHelper;
import org.apache.reef.io.network.group.impl.operators.SenderHelperImpl;
import org.apache.reef.io.network.impl.MessagingTransportFactory;
import org.apache.reef.io.network.impl.NetworkService;
import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.io.network.naming.NameServerImpl;
import org.apache.reef.io.network.proto.ReefNetworkGroupCommProtos;
import org.apache.reef.io.network.util.ListCodec;
import org.apache.reef.io.network.util.StringCodec;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.io.network.util.TestUtils;
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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class SenderTest {

  private static final StringIdentifierFactory idFac = new StringIdentifierFactory();
  private static final int numTasks = 5;
  private static final List<ComparableIdentifier> ids = new ArrayList<>(numTasks);
  private static final String nameServiceAddr = NetUtils.getLocalAddress();
  private static final NameServer nameService = new NameServerImpl(0, idFac);
  private static final int nameServicePort = nameService.getPort();
  private static final List<Integer> nsPorts = new ArrayList<>(numTasks);
  private static final List<BlockingQueue<ReefNetworkGroupCommProtos.GroupCommMessage>> queues = new ArrayList<>(numTasks);
  private static List<NetworkService<ReefNetworkGroupCommProtos.GroupCommMessage>> netServices = new ArrayList<>(numTasks);

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    for (int i = 0; i < numTasks; i++) {
      Identifier id = idFac.getNewInstance("Task" + i);
      ids.add((ComparableIdentifier) id);
      queues.add(new LinkedBlockingQueue<ReefNetworkGroupCommProtos.GroupCommMessage>(5));
    }

    for (int i = 0; i < numTasks; i++) {
      Identifier id = ids.get(i);
      BlockingQueue<ReefNetworkGroupCommProtos.GroupCommMessage> queue = queues.get(i);
      EventHandler<Message<ReefNetworkGroupCommProtos.GroupCommMessage>> recvHandler = new RcvHandler(id, queue);
      EventHandler<Exception> exHandler = new SndExcHandler(id, queue);
      NetworkService<ReefNetworkGroupCommProtos.GroupCommMessage> netService = new NetworkService<>(
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
    for (NetworkService<ReefNetworkGroupCommProtos.GroupCommMessage> netService : netServices)
      netService.close();
    nameService.close();
  }

  /**
   * Test method for {@link org.apache.reef.io.network.group.impl.operators.SenderHelperImpl#SenderHelperImpl(org.apache.reef.io.network.impl.NetworkService, org.apache.reef.wake.remote.Codec)}.
   */
  @Test
  public final void testSenderHelperImpl() {
    SenderHelper<ReefNetworkGroupCommProtos.GroupCommMessage> sender = new SenderHelperImpl<>(netServices.get(0), new GCMCodec());
    Assert.assertNotNull("new SenderHelperImp()", sender);
  }

  /**
   * Test method for {@link org.apache.reef.io.network.group.impl.operators.SenderHelperImpl#send(org.apache.reef.wake.Identifier, org.apache.reef.wake.Identifier, java.lang.Object, org.apache.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type)}.
   */
  @Test
  public final void testSendIdentifierIdentifierTType() throws NetworkException, InterruptedException {
    StringCodec strCodec = new StringCodec();
    for (int i = 0; i < ids.size(); i++) {
      Identifier from = ids.get(i);
      NetworkService<ReefNetworkGroupCommProtos.GroupCommMessage> fromNetService = netServices.get(i);
      SenderHelper<String> sender = new SenderHelperImpl<>(fromNetService,
          strCodec);
      for (int j = 0; j < ids.size(); j++) {
        Identifier to = ids.get(j);
        BlockingQueue<ReefNetworkGroupCommProtos.GroupCommMessage> toQue = queues.get(j);

        for (ReefNetworkGroupCommProtos.GroupCommMessage.Type type : ReefNetworkGroupCommProtos.GroupCommMessage.Type.values()) {
          if (TestUtils.controlMessage(type)) continue;
          String msg = "Hello" + i + j + type;
          byte[] msgBytes = msg.getBytes();
          ReefNetworkGroupCommProtos.GroupCommMessage expected = TestUtils.bldGCM(type, from, to,
              msgBytes);
          sender.send(from, to, msg, type);
          ReefNetworkGroupCommProtos.GroupCommMessage actual = toQue.poll(100, TimeUnit.MILLISECONDS);
          Assert.assertEquals("SenderHelper.send(from,to,T): ", expected,
              actual);
        }
      }
    }
  }

  /**
   * Test method for {@link org.apache.reef.io.network.group.impl.operators.SenderHelperImpl#send(org.apache.reef.wake.Identifier, org.apache.reef.wake.Identifier, java.util.List, org.apache.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type)}.
   */
  @Test
  public final void testSendIdentifierIdentifierListOfTType() throws NetworkException, InterruptedException {
    StringCodec strCodec = new StringCodec();
    for (int i = 0; i < ids.size(); i++) {
      Identifier from = ids.get(i);
      NetworkService<ReefNetworkGroupCommProtos.GroupCommMessage> fromNetService = netServices.get(i);
      SenderHelper<String> sender = new SenderHelperImpl<>(fromNetService,
          strCodec);
      for (int j = 0; j < ids.size(); j++) {
        Identifier to = ids.get(j);
        BlockingQueue<ReefNetworkGroupCommProtos.GroupCommMessage> toQue = queues.get(j);

        for (ReefNetworkGroupCommProtos.GroupCommMessage.Type type : ReefNetworkGroupCommProtos.GroupCommMessage.Type.values()) {
          if (TestUtils.controlMessage(type)) continue;
          List<String> msgs = new ArrayList<>();
          byte[][] msgsBytes = new byte[5][];
          for (int k = 0; k < 5; k++) {
            String msg = "Hello" + i + j + type + k;
            msgs.add(msg);
            byte[] msgBytes = msg.getBytes();
            msgsBytes[k] = msgBytes;
          }

          ReefNetworkGroupCommProtos.GroupCommMessage expected = TestUtils.bldGCM(type, from, to,
              msgsBytes);
          sender.send(from, to, msgs, type);
          ReefNetworkGroupCommProtos.GroupCommMessage actual = toQue.poll(100, TimeUnit.MILLISECONDS);
          Assert.assertEquals("SenderHelper.send(from,to,List<T>): ", expected,
              actual);
        }
      }
    }
  }

  /**
   * Test method for {@link org.apache.reef.io.network.group.impl.operators.SenderHelperImpl#sendListOfList(org.apache.reef.wake.Identifier, org.apache.reef.wake.Identifier, java.util.List, org.apache.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type)}.
   */
  @Test
  public final void testSendListOfList() throws NetworkException, InterruptedException {
    StringCodec strCodec = new StringCodec();
    ListCodec<String> lstCodec = new ListCodec<>(strCodec);
    for (int i = 0; i < ids.size(); i++) {
      Identifier from = ids.get(i);
      NetworkService<ReefNetworkGroupCommProtos.GroupCommMessage> fromNetService = netServices.get(i);
      SenderHelper<String> sender = new SenderHelperImpl<>(fromNetService,
          strCodec);
      for (int j = 0; j < ids.size(); j++) {
        Identifier to = ids.get(j);
        BlockingQueue<ReefNetworkGroupCommProtos.GroupCommMessage> toQue = queues.get(j);

        for (ReefNetworkGroupCommProtos.GroupCommMessage.Type type : ReefNetworkGroupCommProtos.GroupCommMessage.Type.values()) {
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
          ReefNetworkGroupCommProtos.GroupCommMessage expected = TestUtils.bldGCM(type, from, to,
              msgsBytes);
          sender.sendListOfList(from, to, msgss, type);
          ReefNetworkGroupCommProtos.GroupCommMessage actual = toQue.poll(100, TimeUnit.MILLISECONDS);
          Assert.assertEquals("SenderHelper.send(from,to,List<T>): ", expected,
              actual);
        }
      }
    }
  }

  /**
   * Test method for {@link org.apache.reef.io.network.group.impl.operators.SenderHelperImpl#send(org.apache.reef.wake.Identifier, java.util.List, java.util.List, java.util.List, org.apache.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type)}.
   */
  @Test
  public final void testSendIdentifierListOfQextendsIdentifierListOfTListOfIntegerType() throws NetworkException, InterruptedException {
    Random r = new Random(1331);
    final StringCodec strCodec = new StringCodec();
    for (int i = 0; i < ids.size(); i++) {
      Identifier from = ids.get(i);
      NetworkService<ReefNetworkGroupCommProtos.GroupCommMessage> fromNetService = netServices.get(i);
      SenderHelper<String> sender = new SenderHelperImpl<>(fromNetService,
          strCodec);
      List<Identifier> tos = new ArrayList<>(numTasks - 1);
      int[] indexArr = {1, 3, 2, 4, 0};
      for (int j : indexArr) {
        tos.add(ids.get(j));
      }

      for (ReefNetworkGroupCommProtos.GroupCommMessage.Type type : ReefNetworkGroupCommProtos.GroupCommMessage.Type.values()) {
        if (TestUtils.controlMessage(type)) continue;
        final int numMsgs = r.nextInt(100) + 1;
        List<Integer> counts = genRndCounts(ids.size(), numMsgs, r);
        List<String> msgs = new ArrayList<>(numMsgs);
        ReefNetworkGroupCommProtos.GroupCommMessage[] expecteds = new ReefNetworkGroupCommProtos.GroupCommMessage[ids.size()];
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
        ReefNetworkGroupCommProtos.GroupCommMessage[] actuals = new ReefNetworkGroupCommProtos.GroupCommMessage[ids.size()];
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

class RcvHandler implements EventHandler<Message<ReefNetworkGroupCommProtos.GroupCommMessage>> {

  Identifier self;
  BlockingQueue<ReefNetworkGroupCommProtos.GroupCommMessage> queue;

  public RcvHandler(Identifier self, BlockingQueue<ReefNetworkGroupCommProtos.GroupCommMessage> queue) {
    this.self = self;
    this.queue = queue;
  }

  @Override
  public void onNext(Message<ReefNetworkGroupCommProtos.GroupCommMessage> msg) {
    ReefNetworkGroupCommProtos.GroupCommMessage gcm = msg.getData().iterator().next();
    queue.add(gcm);
  }
}

class SndExcHandler implements EventHandler<Exception> {

  Identifier self;
  BlockingQueue<ReefNetworkGroupCommProtos.GroupCommMessage> queue;

  public SndExcHandler(Identifier self, BlockingQueue<ReefNetworkGroupCommProtos.GroupCommMessage> queue) {
    this.self = self;
    this.queue = queue;
  }

  @Override
  public void onNext(Exception exc) {
    ReefNetworkGroupCommProtos.GroupCommMessage gcm = TestUtils.bldGCM(ReefNetworkGroupCommProtos.GroupCommMessage.Type.Scatter, self, self, exc.getMessage().getBytes());
    queue.add(gcm);
  }
}
