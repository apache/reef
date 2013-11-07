/**
 * Copyright (C) 2013 Microsoft Corporation
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
 * @author shravan
 *
 * @param <T>
 */
public class SenderHelperImpl<T> implements SenderHelper<T> {
	NetworkService<GroupCommMessage> netService;
	Codec<T> codec;

	@NamedParameter(doc = "codec for the network service", short_name = "nscodec")
	public static class SenderCodec implements Name<Codec<?>> {
		//intentionally blank
	}

	@Inject
	public SenderHelperImpl(
			NetworkService<GroupCommMessage> netService,
			@Parameter(SenderCodec.class) Codec<T> codec) {
		super();
		this.netService = netService;
		this.codec = codec;
	}

	@Override
	public void send(Identifier from, Identifier to, T element, Type msgType)
			throws NetworkException {
		send(from, to, Arrays.asList(element), msgType);
	}

	@Override
	public void send(Identifier from, Identifier to, List<T> elements,
			Type msgType) throws NetworkException {
		GroupCommMessage.Builder GCMBuilder = GroupCommMessage.newBuilder();
		GCMBuilder.setType(msgType);
		GCMBuilder.setSrcid(from.toString());
		GCMBuilder.setDestid(to.toString());
		GroupMessageBody.Builder bodyBuilder = GroupMessageBody.newBuilder();
		for (T element : elements) {
			bodyBuilder.setData(ByteString.copyFrom(codec.encode(element)));
			GCMBuilder.addMsgs(bodyBuilder.build());
		}
		GroupCommMessage msg = GCMBuilder.build();
		netServiceSend(to, msg);

	}
	
	@Override
	public void sendListOfList(Identifier from, Identifier to, List<List<T>> elements,
			Type msgType) throws NetworkException {
		GroupCommMessage.Builder GCMBuilder = GroupCommMessage.newBuilder();
		GCMBuilder.setType(msgType);
		GCMBuilder.setSrcid(from.toString());
		GCMBuilder.setDestid(to.toString());
		GroupMessageBody.Builder bodyBuilder = GroupMessageBody.newBuilder();
		ListCodec<T> lstCodec = new ListCodec<>(codec);
		for (List<T> subElems : elements) {
			bodyBuilder.setData(ByteString.copyFrom(lstCodec.encode(subElems)));
			GCMBuilder.addMsgs(bodyBuilder.build());
		}
		GroupCommMessage msg = GCMBuilder.build();
		netServiceSend(to, msg);
	}

	private void netServiceSend(Identifier to, GroupCommMessage msg) throws NetworkException {
		Connection<GroupCommMessage> conn = netService.newConnection(to);
		conn.open();
		conn.write(msg);
		//conn.close();
	}

	@Override
	public void send(Identifier from, List<? extends Identifier> to, List<T> elements,
			List<Integer> counts, Type msgType) throws NetworkException {
		int offset = 0;
		Iterator<? extends Identifier> toIter = to.iterator();
		for (int cnt : counts) {
			Identifier dstId = toIter.next();
			List<T> subElems = elements.subList(offset, offset + cnt);
			send(from, dstId, subElems, msgType);
//			System.out.println(from.toString() + " Sending elements(" + offset
//					+ "," + cnt + "): " + subElems + " to " + dstId.toString());
			offset += cnt;
		}
	}

}
