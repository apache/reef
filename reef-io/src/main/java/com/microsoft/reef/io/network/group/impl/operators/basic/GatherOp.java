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
import com.microsoft.reef.io.network.group.operators.Gather;
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
 * Implementation of {@link Gather}
 * @author shravan
 *
 */
public class GatherOp implements Gather {

	public static class Sender<T> extends SenderBase<T> implements Gather.Sender<T> {


		@Inject
		public Sender(
				NetworkService<GroupCommMessage> netService,
				GroupCommNetworkHandler multiHandler,
				@Parameter(GroupParameters.Gather.DataCodec.class) Codec<T> codec,
				@Parameter(GroupParameters.Gather.SenderParams.SelfId.class) String self,
				@Parameter(GroupParameters.Gather.SenderParams.ParentId.class) String parent, 
				@Parameter(GroupParameters.Gather.SenderParams.ChildIds.class) String children,
				@Parameter(GroupParameters.IDFactory.class) IdentifierFactory idFac){
			super(
					new SenderHelperImpl<>(netService, codec),
					new ReceiverHelperImpl<String>(netService, new StringCodec(), multiHandler),
					idFac.getNewInstance(self),
					idFac.getNewInstance(parent),
					(children==GroupParameters.defaultValue) ? null : Utils.parseListCmp(children, idFac)
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
			dataSender.send(getSelf(), getParent(), element, Type.Gather);
			String result = ackReceiver.receive(getParent(), getSelf(), Type.Gather);
			System.out.println(getSelf() + " received: " + result + " from "
					+ getParent());
		}
	}

	public static class Receiver<T> extends ReceiverBase<T> implements Gather.Receiver<T> {
		@Inject
		public Receiver(
				NetworkService<GroupCommMessage> netService,
				GroupCommNetworkHandler multiHandler,
				@Parameter(GroupParameters.Gather.DataCodec.class) Codec<T> codec,
				@Parameter(GroupParameters.Gather.ReceiverParams.SelfId.class) String self,
				@Parameter(GroupParameters.Gather.ReceiverParams.ParentId.class) String parent, 
				@Parameter(GroupParameters.Gather.ReceiverParams.ChildIds.class) String children,
				@Parameter(GroupParameters.IDFactory.class) IdentifierFactory idFac){
			super(
					new ReceiverHelperImpl<>(netService, codec, multiHandler),
					new SenderHelperImpl<>(netService, new StringCodec()),
					idFac.getNewInstance(self),
					(parent==GroupParameters.defaultValue) ? null : idFac.getNewInstance(parent),
					Utils.parseListCmp(children, idFac)
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
		public List<T> receive() throws InterruptedException, NetworkException {
			return receive(getChildren());
		}

		@Override
		public List<T> receive(List<? extends Identifier> order)
				throws InterruptedException, NetworkException {
			List<T> result = dataReceiver.receive(order, getSelf(), Type.Gather);
			System.out.println(getSelf() + " received " + result + " from "
					+ order);
			for (Identifier child : order) {
				ackSender.send(getSelf(), child, "ACK", Type.Gather);
			}
			return result;
		}
	}

}
