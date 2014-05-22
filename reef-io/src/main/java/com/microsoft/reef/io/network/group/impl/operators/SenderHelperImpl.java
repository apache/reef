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
package com.microsoft.reef.io.network.group.impl.operators;

import com.google.protobuf.ByteString;
import com.microsoft.reef.exception.evaluator.NetworkException;
import com.microsoft.reef.io.network.Connection;
import com.microsoft.reef.io.network.impl.NetworkService;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupMessageBody;
import com.microsoft.reef.io.network.util.ListCodec;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.Identifier;
import com.microsoft.wake.remote.Codec;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Implementation of {@link SenderHelper} using point-to-point
 * communication provided by the {@link NetworkService}
 */
public class SenderHelperImpl<T> implements SenderHelper<T> {

  final NetworkService<GroupCommMessage> netService;
  final Codec<T> codec;

  @NamedParameter(doc = "codec for the network service", short_name = "nscodec")
  public static class SenderCodec implements Name<Codec<?>> {
  }

  @Inject
  public SenderHelperImpl(final NetworkService<GroupCommMessage> netService,
                          final @Parameter(SenderCodec.class) Codec<T> codec) {
    super();
    this.netService = netService;
    this.codec = codec;
  }

  @Override
  public void send(final Identifier from, final Identifier to,
                   final T element, final Type msgType) throws NetworkException {
    send(from, to, Arrays.asList(element), msgType);
  }

  @Override
  public void send(final Identifier from, final Identifier to,
                   final List<T> elements, final Type msgType) throws NetworkException {
    final GroupCommMessage.Builder GCMBuilder = GroupCommMessage.newBuilder();
    GCMBuilder.setType(msgType);
    GCMBuilder.setSrcid(from.toString());
    GCMBuilder.setDestid(to.toString());
    final GroupMessageBody.Builder bodyBuilder = GroupMessageBody.newBuilder();
    for (final T element : elements) {
      bodyBuilder.setData(ByteString.copyFrom(this.codec.encode(element)));
      GCMBuilder.addMsgs(bodyBuilder.build());
    }
    GroupCommMessage msg = GCMBuilder.build();
    netServiceSend(to, msg);
  }

  @Override
  public void sendListOfList(final Identifier from, final Identifier to,
                             final List<List<T>> elements, final Type msgType)
      throws NetworkException {
    final GroupCommMessage.Builder GCMBuilder = GroupCommMessage.newBuilder();
    GCMBuilder.setType(msgType);
    GCMBuilder.setSrcid(from.toString());
    GCMBuilder.setDestid(to.toString());
    final GroupMessageBody.Builder bodyBuilder = GroupMessageBody.newBuilder();
    final ListCodec<T> lstCodec = new ListCodec<>(this.codec);
    for (final List<T> subElems : elements) {
      bodyBuilder.setData(ByteString.copyFrom(lstCodec.encode(subElems)));
      GCMBuilder.addMsgs(bodyBuilder.build());
    }
    GroupCommMessage msg = GCMBuilder.build();
    netServiceSend(to, msg);
  }

  private void netServiceSend(Identifier to, GroupCommMessage msg) throws NetworkException {
    final Connection<GroupCommMessage> conn = this.netService.newConnection(to);
    conn.open();
    conn.write(msg);
    //conn.close();
  }

  @Override
  public void send(final Identifier from, List<? extends Identifier> to, final List<T> elements,
                   final List<Integer> counts, final Type msgType) throws NetworkException {
    int offset = 0;
    final Iterator<? extends Identifier> toIter = to.iterator();
    for (final int cnt : counts) {
      final Identifier dstId = toIter.next();
      final List<T> subElems = elements.subList(offset, offset + cnt);
      send(from, dstId, subElems, msgType);
      // LOG.log(Level.FINEST, "{0} Sending elements({1}, {2}): {3} to {4}",
      //         new Object[] { from, offset, cnt, subElems, dstId });
      offset += cnt;
    }
  }
}
