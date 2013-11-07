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
import com.microsoft.reef.io.network.group.impl.operators.basic.config.GroupParameters;
import com.microsoft.reef.io.network.group.operators.Gather;
import com.microsoft.reef.io.network.group.operators.Reduce;
import com.microsoft.reef.io.network.impl.NetworkService;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.reef.io.network.util.Utils;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.ComparableIdentifier;
import com.microsoft.wake.Identifier;
import com.microsoft.wake.IdentifierFactory;
import com.microsoft.wake.remote.Codec;

import javax.inject.Inject;
import java.util.List;

/**
 * Implementation of {@link Reduce}
 * @author shravan
 *
 */
public class ReduceOp implements Reduce {
	public static class Sender<T> extends SenderReceiverBase implements Reduce.Sender<T> {
		Gather.Sender<T> gatherSender;
		Reduce.ReduceFunction<T> redFunc;
		
		@Inject
		public Sender(
				NetworkService<GroupCommMessage> netService,
				GroupCommNetworkHandler multiHandler,
				@Parameter(GroupParameters.Reduce.DataCodec.class) Codec<T> codec,
				@Parameter(GroupParameters.Reduce.SenderParams.SelfId.class) String self,
				@Parameter(GroupParameters.Reduce.SenderParams.ParentId.class) String parent, 
				@Parameter(GroupParameters.Reduce.SenderParams.ChildIds.class) String children,
				@Parameter(GroupParameters.IDFactory.class) IdentifierFactory idFac,
				@Parameter(GroupParameters.Reduce.ReduceFunction.class) ReduceFunction<T> redFunc){
			this(
					new GatherOp.Sender<>(netService, multiHandler, codec, self, parent, children, idFac),
					idFac.getNewInstance(self),
					idFac.getNewInstance(parent),
					(children.equals(GroupParameters.defaultValue)) ? null : Utils.parseListCmp(children, idFac),
					redFunc
				);
		}
		
		public Sender(Gather.Sender<T> gatherSender, Identifier self,
				Identifier parent, List<ComparableIdentifier> children,
				ReduceFunction<T> redFunc) {
			super(self,parent,children);
			this.gatherSender = gatherSender;
			this.redFunc = redFunc;
		}

		@Override
		public void send(T element) throws NetworkException,
				InterruptedException {
			gatherSender.send(element);
		}

		@Override
		public Reduce.ReduceFunction<T> getReduceFunction() {
			return redFunc;
		}
	}

	public static class Receiver<T> extends SenderReceiverBase implements Reduce.Receiver<T> {
		Gather.Receiver<T> gatherReceiver;
		Reduce.ReduceFunction<T> redFunc;
		
		@Inject
		public Receiver(
				NetworkService<GroupCommMessage> netService,
				GroupCommNetworkHandler multiHandler,
				@Parameter(GroupParameters.Reduce.DataCodec.class) Codec<T> codec,
				@Parameter(GroupParameters.Reduce.ReceiverParams.SelfId.class) String self,
				@Parameter(GroupParameters.Reduce.ReceiverParams.ParentId.class) String parent, 
				@Parameter(GroupParameters.Reduce.ReceiverParams.ChildIds.class) String children,
				@Parameter(GroupParameters.IDFactory.class) IdentifierFactory idFac,
				@Parameter(GroupParameters.Reduce.ReduceFunction.class) ReduceFunction<T> redFunc){
			this(
					new GatherOp.Receiver<>(netService, multiHandler, codec, self, parent, children, idFac),
					idFac.getNewInstance(self),
					(parent.equals(GroupParameters.defaultValue)) ? null : idFac.getNewInstance(parent),
					Utils.parseListCmp(children, idFac),
					redFunc
				);
		}

		public Receiver(Gather.Receiver<T> gatherReceiver, Identifier self,
				Identifier parent, List<ComparableIdentifier> children,
				ReduceFunction<T> redFunc) {
			super(self,parent,children);
			this.gatherReceiver = gatherReceiver;
			this.redFunc = redFunc;
		}

		@Override
		public T reduce() throws InterruptedException, NetworkException {
			return reduce(getChildren());
		}

		@Override
		public Reduce.ReduceFunction<T> getReduceFunction() {
			return redFunc;
		}

		@Override
		public T reduce(List<? extends Identifier> order)
				throws InterruptedException, NetworkException {
			List<T> result = gatherReceiver.receive(order);
			T redRes = redFunc.apply(result);
			return redRes;
		}
	}
}
