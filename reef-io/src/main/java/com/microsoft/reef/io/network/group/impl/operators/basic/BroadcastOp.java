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
package com.microsoft.reef.io.network.group.impl.operators.basic;

import com.microsoft.reef.exception.evaluator.NetworkException;
import com.microsoft.reef.io.network.group.impl.GroupCommNetworkHandler;
import com.microsoft.reef.io.network.group.impl.operators.ReceiverHelper;
import com.microsoft.reef.io.network.group.impl.operators.ReceiverHelperImpl;
import com.microsoft.reef.io.network.group.impl.operators.SenderHelper;
import com.microsoft.reef.io.network.group.impl.operators.SenderHelperImpl;
import com.microsoft.reef.io.network.group.impl.operators.basic.config.GroupParameters;
import com.microsoft.reef.io.network.group.operators.Broadcast;
import com.microsoft.reef.io.network.impl.NetworkService;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type;
import com.microsoft.reef.io.network.util.StringCodec;
import com.microsoft.reef.io.network.util.Utils;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.ComparableIdentifier;
import com.microsoft.wake.Identifier;
import com.microsoft.wake.IdentifierFactory;
import com.microsoft.wake.remote.Codec;

import javax.inject.Inject;
import java.util.List;

/**
 * Implementation of {@link Broadcast}
 * @author shravan
 *
 */
public class BroadcastOp implements Broadcast {

	public static class Sender<T> extends SenderBase<T> implements Broadcast.Sender<T> {
		@Inject
		public Sender(
				NetworkService<GroupCommMessage> netService,
				GroupCommNetworkHandler multiHandler,
				@Parameter(GroupParameters.BroadCast.DataCodec.class) Codec<T> codec,
				@Parameter(GroupParameters.BroadCast.SenderParams.SelfId.class) String self,
				@Parameter(GroupParameters.BroadCast.SenderParams.ParentId.class) String parent, 
				@Parameter(GroupParameters.BroadCast.SenderParams.ChildIds.class) String children,
				@Parameter(GroupParameters.IDFactory.class) IdentifierFactory idFac){
			super(
					new SenderHelperImpl<>(netService, codec),
					new ReceiverHelperImpl<String>(netService, new StringCodec(), multiHandler),
					idFac.getNewInstance(self),
					(parent.equals(GroupParameters.defaultValue)) ? null : idFac.getNewInstance(parent),
					Utils.parseListCmp(children, idFac)
				);
		}
		
		public Sender(
				SenderHelper<T> dataSender,
				ReceiverHelper<String> ackReceiver, 
				Identifier self, Identifier parent,
				List<ComparableIdentifier> children) {
			super(dataSender, ackReceiver, self, parent, children);
		}

		@Override
		public void send(T element) throws NetworkException,
				InterruptedException {
			for (Identifier child : getChildren()) {
				dataSender.send(getSelf(), child, element, Type.Broadcast);
			}
			List<String> result = ackReceiver.receive(getChildren(), getSelf(),
					Type.Broadcast);
			System.out.println(getSelf() + " received: " + result + " from "
					+ getChildren());
		}
	}

	public static class Receiver<T> extends ReceiverBase<T> implements Broadcast.Receiver<T> {
		@Inject
		public Receiver(
				NetworkService<GroupCommMessage> netService,
				GroupCommNetworkHandler multiHandler,
				@Parameter(GroupParameters.BroadCast.DataCodec.class) Codec<T> codec,
				@Parameter(GroupParameters.BroadCast.ReceiverParams.SelfId.class) String self,
				@Parameter(GroupParameters.BroadCast.ReceiverParams.ParentId.class) String parent, 
				@Parameter(GroupParameters.BroadCast.ReceiverParams.ChildIds.class) String children,
				@Parameter(GroupParameters.IDFactory.class) IdentifierFactory idFac){
			super(
					new ReceiverHelperImpl<>(netService, codec, multiHandler),
					new SenderHelperImpl<>(netService, new StringCodec()),
					idFac.getNewInstance(self),
					idFac.getNewInstance(parent),
					(children.equals(GroupParameters.defaultValue)) ? null : Utils.parseListCmp(children, idFac)
				);
		}
		
		public Receiver(
				ReceiverHelper<T> dataReceiver,
				SenderHelper<String> ackSender, 
				Identifier self, Identifier parent,
				List<ComparableIdentifier> children) {
			super(dataReceiver, ackSender, self, parent, children);
		}

		@Override
		public T receive() throws NetworkException, InterruptedException {
			T result = dataReceiver.receive(getParent(), getSelf(), Type.Broadcast);
			System.out.println(getSelf() + " received: " + result + " from "
					+ getParent());
			ackSender.send(getSelf(), getParent(), "ACK", Type.Broadcast);
			return result;
		}
	}
}
