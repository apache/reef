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
package org.apache.reef.services.network;

import org.apache.reef.io.network.*;
import org.apache.reef.io.network.exception.NetworkRuntimeException;
import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.io.network.naming.NameServerImpl;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.services.network.util.Monitor;
import org.apache.reef.services.network.util.NetworkServiceUtil;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.Codec;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * NetworkService test for exceptional cases
 */
public class NetworkServiceExceptionTest {

  private static final Logger LOG = Logger.getLogger(NewNetworkServiceTest.class.getName());

  /**
   * Exception test in sending messages to closed connection
   * It checks whether onSuccess or onException is called for all sent messages
   *
   * @throws Exception
   */
  @Test
  public void testExceptionInSendingMessagesToClosedConnection() throws Exception {
    final IdentifierFactory idFactory = new StringIdentifierFactory();
    final NameServer nameServer = new NameServerImpl(0, idFactory);
    final int nameServerPort = nameServer.getPort();

    Set<Codec<?>> codecs = new HashSet<>();
    codecs.add(new IntegerCodec());

    final int senderThreadNum = 20;
    final int senderNum = 5;
    final int receiverNum = 5;
    final int messagePerSenderWithOneThread = 300;
    final int totalEventNumber = messagePerSenderWithOneThread * senderNum * senderThreadNum;
    final AtomicInteger sentNum = new AtomicInteger(0);
    final Monitor monitor = new Monitor();

    final TestNetworkEventHandler[] eventHandlers = new TestNetworkEventHandler[senderNum + receiverNum];
    final TestNetworkLinkListener[] linkListeners = new TestNetworkLinkListener[senderNum + receiverNum];
    final org.apache.reef.io.network.NetworkService[] senderArr = new org.apache.reef.io.network.NetworkService[senderNum];
    final org.apache.reef.io.network.NetworkService[] receiverArr = new org.apache.reef.io.network.NetworkService[receiverNum];
    final Identifier[] receiverIdArr = new Identifier[receiverNum];

    final Set<String> networkEvents = new HashSet<>();
    networkEvents.add(Integer.class.getName());

    for (int i = 0; i < senderNum; i++) {
      eventHandlers[i] = new TestNetworkEventHandler();
      linkListeners[i] = new TestNetworkLinkListener(monitor, sentNum, totalEventNumber);

      Set<NetworkEventHandler<?>> handlers = new HashSet<>();
      handlers.add(eventHandlers[i]);
      Set<NetworkLinkListener<?>> exceptionHandlerSet = new HashSet<>();
      exceptionHandlerSet.add(linkListeners[i]);

      senderArr[i] = NetworkServiceUtil.getTestNetworkService(
          networkEvents,
          handlers,
          codecs,
          exceptionHandlerSet,
          "sender" + i,
          nameServerPort
      );
    }

    for (int i = 0; i < receiverNum; i++) {
      eventHandlers[i + senderNum] = new TestNetworkEventHandler();
      linkListeners[i + senderNum] = new TestNetworkLinkListener(monitor, sentNum, totalEventNumber);

      Set<NetworkEventHandler<?>> handlers = new HashSet<>();
      handlers.add(eventHandlers[i + senderNum]);
      Set<NetworkLinkListener<?>> exceptionHandlerSet = new HashSet<>();
      exceptionHandlerSet.add(linkListeners[i + senderNum]);

      receiverArr[i] = NetworkServiceUtil.getTestNetworkService(
          networkEvents,
          handlers,
          codecs,
          exceptionHandlerSet,
          "receiver" + i,
          nameServerPort
      );

      receiverIdArr[i] = idFactory.getNewInstance("receiver" + i);
    }

    final Random rand = new Random();
    long start = System.currentTimeMillis();
    final AtomicInteger completedThread = new AtomicInteger(0);
    final AtomicInteger sentEventCount = new AtomicInteger(0);
    final AtomicBoolean isTimeToCauseException = new AtomicBoolean(false);
    for (int i = 0; i < senderThreadNum; i++) {
      new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            for (int j = 0; j < messagePerSenderWithOneThread; j++) {
              for (int i = 0; i < senderNum; i++) {
                if (!isTimeToCauseException.get()) {
                  if (sentEventCount.incrementAndGet() >= totalEventNumber * 0.95) {
                    synchronized (isTimeToCauseException) {
                      isTimeToCauseException.set(true);
                      isTimeToCauseException.notify();
                    }
                  }
                }
                senderArr[i].sendEvent(receiverIdArr[j % receiverNum], rand.nextInt());
              }
            }
          } catch (Exception e) {
            e.printStackTrace();
            throw new NetworkRuntimeException(e);
          }
          LOG.log(Level.FINE, "\n\n*************** thread " + completedThread.incrementAndGet() + " complete ******************\n");
        }
      }).start();
    }

    synchronized (isTimeToCauseException) {
      if (!isTimeToCauseException.get()) {
        isTimeToCauseException.wait();
      }
    }

    receiverArr[0].close();
    receiverArr[receiverNum-1].close();

    monitor.mwait();

    for (int i = 0; i < senderNum; i++) {
      senderArr[i].close();
    }

    for (int i = 0; i < receiverNum; i++) {
      if (i != 0 && i != receiverNum - 1) {
        receiverArr[i].close();
      }
    }

    nameServer.close();

    int successfullySentNumOfSenders = 0;
    int failedNumOfSenders = 0;

    for (int i = 0; i < linkListeners.length; i++) {
      successfullySentNumOfSenders += linkListeners[i].getSuccessfullySentNum();
      failedNumOfSenders += linkListeners[i].getFaildNum();
    }

    Assert.assertTrue((successfullySentNumOfSenders + failedNumOfSenders) == totalEventNumber);

    long end = System.currentTimeMillis();

    LOG.log(Level.INFO, "sender count: " + senderNum + " sender thread count: " + senderThreadNum + " receiver count: "
        + receiverNum);
    LOG.log(Level.INFO, "runtime: " + (end - start) / 1000.0 + "s total message :" + totalEventNumber +
        " success count: " + successfullySentNumOfSenders + " exception count: " + failedNumOfSenders);
  }

  class TestNetworkEventHandler implements NetworkEventHandler<Integer> {

    TestNetworkEventHandler() {
    }

    @Override
    public void onNext(NetworkEvent<Integer> value) {
    }
  }

  class TestNetworkLinkListener implements NetworkLinkListener<Integer> {

    private final Monitor monitor;
    private final int expectedNum;
    private final AtomicInteger sentNum;
    private final AtomicInteger successfullySentNum;
    private final AtomicInteger faildSentNum;

    TestNetworkLinkListener(final Monitor monitor, final AtomicInteger sentNum, final int expectedNum) {
      this.monitor = monitor;
      this.expectedNum = expectedNum;
      this.sentNum = sentNum;
      this.successfullySentNum = new AtomicInteger(0);
      this.faildSentNum = new AtomicInteger(0);
    }

    @Override
    public void onSuccess(Identifier remoteId, List<Integer> messageList) {
      successfullySentNum.getAndIncrement();
      if (sentNum.incrementAndGet() == expectedNum) {
        monitor.mnotify();
      }
    }

    @Override
    public void onException(Throwable cause, Identifier remoteId, List<Integer> messageList) {
      faildSentNum.getAndIncrement();
      if (sentNum.incrementAndGet() == expectedNum) {
        monitor.mnotify();
      }
    }

    public int getSuccessfullySentNum() {
      return successfullySentNum.get();
    }

    public int getFaildNum() {
      return faildSentNum.get();
    }
  }

  class IntegerCodec implements Codec<Integer> {

    @Override
    public Integer decode(byte[] data) {
      return ByteBuffer.wrap(data).getInt();
    }

    @Override
    public byte[] encode(Integer obj) {
      return ByteBuffer.allocate(4).putInt(obj).array();
    }
  }
}
