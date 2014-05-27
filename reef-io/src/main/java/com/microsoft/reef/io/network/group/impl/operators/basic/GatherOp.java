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
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implementation of {@link Gather}
 */
public class GatherOp implements Gather {

  private static final Logger LOG = Logger.getLogger(GatherOp.class.getName());

  public final static class Sender<T> extends SenderBase<T> implements Gather.Sender<T> {

    @Inject
    public Sender(
        final NetworkService<GroupCommMessage> netService,
        final GroupCommNetworkHandler multiHandler,
        final @Parameter(GroupParameters.Gather.DataCodec.class) Codec<T> codec,
        final @Parameter(GroupParameters.Gather.SenderParams.SelfId.class) String self,
        final @Parameter(GroupParameters.Gather.SenderParams.ParentId.class) String parent,
        final @Parameter(GroupParameters.Gather.SenderParams.ChildIds.class) String children,
        final @Parameter(GroupParameters.IDFactory.class) IdentifierFactory idFac) {
      super(new SenderHelperImpl<>(netService, codec),
            new ReceiverHelperImpl<>(netService, new StringCodec(), multiHandler),
            idFac.getNewInstance(self),
            idFac.getNewInstance(parent),
            children == GroupParameters.defaultValue ? null : Utils.parseListCmp(children, idFac));
    }

    public Sender(
        final SenderHelper<T> dataSender,
        final ReceiverHelper<String> ackReceiver,
        final Identifier self, final Identifier parent,
        final List<ComparableIdentifier> children) {
      super(dataSender, ackReceiver, self, parent, children);
    }

    @Override
    public void send(final T element) throws NetworkException, InterruptedException {
      this.dataSender.send(getSelf(), getParent(), element, Type.Gather);
      final String result = this.ackReceiver.receive(getParent(), getSelf(), Type.Gather);
      LOG.log(Level.FINEST, "{0} received: {1} from {2}",
          new Object[] { getSelf(), result, getParent() });
    }
  }

  public static class Receiver<T> extends ReceiverBase<T> implements Gather.Receiver<T> {
    @Inject
    public Receiver(
        final NetworkService<GroupCommMessage> netService,
        final GroupCommNetworkHandler multiHandler,
        final @Parameter(GroupParameters.Gather.DataCodec.class) Codec<T> codec,
        final @Parameter(GroupParameters.Gather.ReceiverParams.SelfId.class) String self,
        final @Parameter(GroupParameters.Gather.ReceiverParams.ParentId.class) String parent,
        final @Parameter(GroupParameters.Gather.ReceiverParams.ChildIds.class) String children,
        final @Parameter(GroupParameters.IDFactory.class) IdentifierFactory idFac) {
      super(new ReceiverHelperImpl<>(netService, codec, multiHandler),
            new SenderHelperImpl<>(netService, new StringCodec()),
            idFac.getNewInstance(self),
            parent == GroupParameters.defaultValue ? null : idFac.getNewInstance(parent),
            Utils.parseListCmp(children, idFac));
    }

    public Receiver(
        final ReceiverHelper<T> dataReceiver,
        final SenderHelper<String> ackSender,
        final Identifier self, final Identifier parent,
        final List<ComparableIdentifier> children) {
      super(dataReceiver, ackSender, self, parent, children);
    }

    @Override
    public List<T> receive() throws InterruptedException, NetworkException {
      return receive(getChildren());
    }

    @Override
    public List<T> receive(final List<? extends Identifier> order)
        throws InterruptedException, NetworkException {
      final List<T> result = this.dataReceiver.receive(order, getSelf(), Type.Gather);
      LOG.log(Level.FINEST, "{0} received: {1} from {2}",
          new Object[] { getSelf(), result, order });
      for (final Identifier child : order) {
        this.ackSender.send(getSelf(), child, "ACK", Type.Gather);
      }
      return result;
    }
  }
}
