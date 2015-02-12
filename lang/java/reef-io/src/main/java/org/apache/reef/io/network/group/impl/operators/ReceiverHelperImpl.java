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
package org.apache.reef.io.network.group.impl.operators;

import org.apache.reef.io.network.group.impl.GroupCommNetworkHandler;
import org.apache.reef.io.network.group.impl.Handler;
import org.apache.reef.io.network.impl.NetworkService;
import org.apache.reef.io.network.proto.ReefNetworkGroupCommProtos;
import org.apache.reef.io.network.util.ListCodec;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.remote.Codec;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of {@link ReceiverHelper} using point-to-point
 * communication provided by the {@link NetworkService}
 *
 * @param <T>
 */
public class ReceiverHelperImpl<T> implements ReceiverHelper<T> {

  NetworkService<ReefNetworkGroupCommProtos.GroupCommMessage> netService;
  Codec<T> codec;
  GroupCommNetworkHandler multiHandler;

  @NamedParameter(doc = "codec for the network service", short_name = "nscodec")
  public static class ReceiverCodec implements Name<Codec<?>> {
    //intentionally blank
  }

  @Inject
  public ReceiverHelperImpl(
      NetworkService<ReefNetworkGroupCommProtos.GroupCommMessage> netService,
      @Parameter(ReceiverCodec.class) Codec<T> codec,
      GroupCommNetworkHandler multiHandler) {
    super();
    this.netService = netService;
    this.codec = codec;
    this.multiHandler = multiHandler;
  }

  @Override
  public T receive(Identifier from, Identifier to, ReefNetworkGroupCommProtos.GroupCommMessage.Type msgType)
      throws InterruptedException {
    return receiveList(from, to, msgType).get(0);
  }

  @Override
  public List<T> receiveList(Identifier from, Identifier to, ReefNetworkGroupCommProtos.GroupCommMessage.Type msgType)
      throws InterruptedException {
    Handler handler = multiHandler.getHandler(msgType);
    ReefNetworkGroupCommProtos.GroupCommMessage data = handler.getData(from);
    List<T> retVal = new ArrayList<>();
    for (ReefNetworkGroupCommProtos.GroupMessageBody body : data.getMsgsList()) {
      retVal.add(codec.decode(body.getData().toByteArray()));
    }
    return retVal;
  }

  @Override
  public List<List<T>> receiveListOfList(Identifier from, Identifier to, ReefNetworkGroupCommProtos.GroupCommMessage.Type msgType)
      throws InterruptedException {
    Handler handler = multiHandler.getHandler(msgType);
    ReefNetworkGroupCommProtos.GroupCommMessage data = handler.getData(from);
    ListCodec<T> lstCodec = new ListCodec<>(codec);
    List<List<T>> retVal = new ArrayList<>();
    for (ReefNetworkGroupCommProtos.GroupMessageBody body : data.getMsgsList()) {
      retVal.add(lstCodec.decode(body.getData().toByteArray()));
    }
    return retVal;
  }

  @Override
  public List<T> receive(List<? extends Identifier> from, Identifier to, ReefNetworkGroupCommProtos.GroupCommMessage.Type msgType)
      throws InterruptedException {
    List<T> result = new ArrayList<>();
    for (Identifier id : from)
      result.add(receive(id, to, msgType));
    return result;
  }

}
