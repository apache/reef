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

import com.google.protobuf.ByteString;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.Connection;
import org.apache.reef.io.network.impl.NetworkService;
import org.apache.reef.io.network.proto.ReefNetworkGroupCommProtos;
import org.apache.reef.io.network.util.ListCodec;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.remote.Codec;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Implementation of {@link SenderHelper} using point-to-point
 * communication provided by the {@link NetworkService}
 */
public class SenderHelperImpl<T> implements SenderHelper<T> {

  final NetworkService<ReefNetworkGroupCommProtos.GroupCommMessage> netService;
  final Codec<T> codec;

  @NamedParameter(doc = "codec for the network service", short_name = "nscodec")
  public static class SenderCodec implements Name<Codec<?>> {
  }

  @Inject
  public SenderHelperImpl(final NetworkService<ReefNetworkGroupCommProtos.GroupCommMessage> netService,
                          final @Parameter(SenderCodec.class) Codec<T> codec) {
    super();
    this.netService = netService;
    this.codec = codec;
  }

  @Override
  public void send(final Identifier from, final Identifier to,
                   final T element, final ReefNetworkGroupCommProtos.GroupCommMessage.Type msgType) throws NetworkException {
    send(from, to, Arrays.asList(element), msgType);
  }

  @Override
  public void send(final Identifier from, final Identifier to,
                   final List<T> elements, final ReefNetworkGroupCommProtos.GroupCommMessage.Type msgType) throws NetworkException {
    final ReefNetworkGroupCommProtos.GroupCommMessage.Builder GCMBuilder = ReefNetworkGroupCommProtos.GroupCommMessage.newBuilder();
    GCMBuilder.setType(msgType);
    GCMBuilder.setSrcid(from.toString());
    GCMBuilder.setDestid(to.toString());
    final ReefNetworkGroupCommProtos.GroupMessageBody.Builder bodyBuilder = ReefNetworkGroupCommProtos.GroupMessageBody.newBuilder();
    for (final T element : elements) {
      bodyBuilder.setData(ByteString.copyFrom(this.codec.encode(element)));
      GCMBuilder.addMsgs(bodyBuilder.build());
    }
    ReefNetworkGroupCommProtos.GroupCommMessage msg = GCMBuilder.build();
    netServiceSend(to, msg);
  }

  @Override
  public void sendListOfList(final Identifier from, final Identifier to,
                             final List<List<T>> elements, final ReefNetworkGroupCommProtos.GroupCommMessage.Type msgType)
      throws NetworkException {
    final ReefNetworkGroupCommProtos.GroupCommMessage.Builder GCMBuilder = ReefNetworkGroupCommProtos.GroupCommMessage.newBuilder();
    GCMBuilder.setType(msgType);
    GCMBuilder.setSrcid(from.toString());
    GCMBuilder.setDestid(to.toString());
    final ReefNetworkGroupCommProtos.GroupMessageBody.Builder bodyBuilder = ReefNetworkGroupCommProtos.GroupMessageBody.newBuilder();
    final ListCodec<T> lstCodec = new ListCodec<>(this.codec);
    for (final List<T> subElems : elements) {
      bodyBuilder.setData(ByteString.copyFrom(lstCodec.encode(subElems)));
      GCMBuilder.addMsgs(bodyBuilder.build());
    }
    ReefNetworkGroupCommProtos.GroupCommMessage msg = GCMBuilder.build();
    netServiceSend(to, msg);
  }

  private void netServiceSend(Identifier to, ReefNetworkGroupCommProtos.GroupCommMessage msg) throws NetworkException {
    final Connection<ReefNetworkGroupCommProtos.GroupCommMessage> conn = this.netService.newConnection(to);
    conn.open();
    conn.write(msg);
    //conn.close();
  }

  @Override
  public void send(final Identifier from, List<? extends Identifier> to, final List<T> elements,
                   final List<Integer> counts, final ReefNetworkGroupCommProtos.GroupCommMessage.Type msgType) throws NetworkException {
    int offset = 0;
    final Iterator<? extends Identifier> toIter = to.iterator();
    for (final int cnt : counts) {
      final Identifier dstId = toIter.next();
      final List<T> subElems = elements.subList(offset, offset + cnt);
      send(from, dstId, subElems, msgType);
      offset += cnt;
    }
  }
}
