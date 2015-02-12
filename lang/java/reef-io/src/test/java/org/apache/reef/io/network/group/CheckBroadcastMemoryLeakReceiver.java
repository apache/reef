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
import org.apache.reef.io.network.group.impl.ExceptionHandler;
import org.apache.reef.io.network.group.impl.GCMCodec;
import org.apache.reef.io.network.group.impl.operators.faulty.*;
import org.apache.reef.io.network.group.operators.Reduce.ReduceFunction;
import org.apache.reef.io.network.impl.MessagingTransportFactory;
import org.apache.reef.io.network.impl.NetworkService;
import org.apache.reef.io.network.proto.ReefNetworkGroupCommProtos;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.Codec;
import org.apache.reef.wake.remote.NetUtils;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CheckBroadcastMemoryLeakReceiver {

  private static final Logger LOG = Logger.getLogger(CheckBroadcastMemoryLeakReceiver.class.getName());

  private static final IdentifierFactory idFac = new StringIdentifierFactory();
  private static final String nameServiceAddr = NetUtils.getLocalAddress();
  private static final int nameServicePort = 5678;

  public static void main(String[] args) throws Exception {

    final Identifier senderId = idFac.getNewInstance(args[0]);
    final Identifier receiverId = idFac.getNewInstance(args[1]);
    boolean leaf = receiverId.toString().indexOf("LEAF") != -1;

    final Codec<byte[]> baCodec = new Codec<byte[]>() {

      @Override
      public byte[] decode(byte[] arr) {
        return arr;
      }

      @Override
      public byte[] encode(byte[] arr) {
        return arr;
      }
    };

    EventHandler<Exception> excHandler = new ExceptionHandler();
    org.apache.reef.io.network.group.impl.operators.faulty.ExceptionHandler redExcHandler = new org.apache.reef.io.network.group.impl.operators.faulty.ExceptionHandler();
    final Set<String> sndIds = Collections.singleton(senderId.toString());
    Set<String> rcvChildren = null;
    Set<String> rcvIds = new HashSet<>(sndIds);

    if (leaf)
      rcvChildren = Collections.singleton(BroadReduceConfig.defaultValue);
    else {
      rcvChildren = new HashSet<>();
      rcvChildren.add("RECEIVERLEAF1");
      rcvChildren.add("RECEIVERLEAF2");
      rcvIds.addAll(rcvChildren);
    }

    final BroadcastHandler rcvHandler = new BroadcastHandler(rcvIds, idFac);
    final ReduceHandler sndHandler = new ReduceHandler(rcvIds, idFac);
    final BroadRedHandler rcvSndHandler = new BroadRedHandler(rcvHandler, sndHandler);
    final NetworkService<ReefNetworkGroupCommProtos.GroupCommMessage> recvService = new NetworkService<>(idFac, 0, nameServiceAddr, nameServicePort, new GCMCodec(), new MessagingTransportFactory(), rcvSndHandler, excHandler);
    recvService.registerId(receiverId);
    final BroadcastOp.Receiver<byte[]> brdCstReceiver = new BroadcastOp.Receiver<>(recvService, rcvHandler, baCodec, receiverId.toString(), senderId.toString(), rcvChildren, idFac);
    ReduceFunction<byte[]> redFunc = new ReduceFunction<byte[]>() {

      @Override
      public byte[] apply(Iterable<byte[]> elements) {
        return elements.iterator().next();
      }
    };
    final ReduceOp.Sender<byte[]> reduceSener = new ReduceOp.Sender<>(recvService, sndHandler, baCodec, redFunc, receiverId.toString(), senderId.toString(), rcvChildren, false, idFac, redExcHandler);
    final int iterations = 100;
    for (int i = 0; i < iterations; i++) {
      try {
        byte[] b = brdCstReceiver.receive();
        reduceSener.send(b);
        LOG.log(Level.FINEST, "Received {0}", i + 1);
      } catch (final NetworkException | InterruptedException e) {
        LOG.log(Level.SEVERE, "Exception", e);
      }
    }
    Thread.sleep(10000);
    recvService.unregisterId(receiverId);
    recvService.close();
    System.exit(0);
  }
}
