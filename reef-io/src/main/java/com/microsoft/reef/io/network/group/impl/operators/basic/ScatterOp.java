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
import com.microsoft.reef.io.network.group.operators.Scatter;
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
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implementation of {@link Scatter}
 *
 * @author shravan
 */
public class ScatterOp implements Scatter {
  private static Logger LOG = Logger.getLogger(ScatterOp.class.getName());

  public static class Sender<T> extends SenderBase<T> implements Scatter.Sender<T> {
    @Inject
    public Sender(
        NetworkService<GroupCommMessage> netService,
        GroupCommNetworkHandler multiHandler,
        @Parameter(GroupParameters.Scatter.DataCodec.class) Codec<T> codec,
        @Parameter(GroupParameters.Scatter.SenderParams.SelfId.class) String self,
        @Parameter(GroupParameters.Scatter.SenderParams.ParentId.class) String parent,
        @Parameter(GroupParameters.Scatter.SenderParams.ChildIds.class) String children,
        @Parameter(GroupParameters.IDFactory.class) IdentifierFactory idFac) {
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
    public void send(List<T> elements,
                     List<Integer> counts,
                     List<? extends Identifier> order)
        throws NetworkException, InterruptedException {
      dataSender.send(getSelf(), order, elements, counts, Type.Scatter);
      List<String> result = ackReceiver.receive(order, getSelf(),
          Type.Scatter);
      LOG.log(Level.FINEST, getSelf() + " received: " + result + " from "
          + getChildren());
    }

    @Override
    public void send(List<T> elements) throws NetworkException,
        InterruptedException {
      List<Integer> counts = Utils.createUniformCounts(elements.size(), getChildren().size());
      send(elements, counts, getChildren());
    }

    @Override
    public void send(List<T> elements, Integer... counts)
        throws NetworkException, InterruptedException {
      send(elements, Arrays.asList(counts), getChildren());
    }

    @Override
    public void send(List<T> elements, List<? extends Identifier> order)
        throws NetworkException, InterruptedException {
      List<Integer> counts = Utils.createUniformCounts(elements.size(), getChildren().size());
      send(elements, counts, order);
    }


  }

  public static class Receiver<T> extends ReceiverBase<T> implements Scatter.Receiver<T> {

    @Inject
    public Receiver(
        NetworkService<GroupCommMessage> netService,
        GroupCommNetworkHandler multiHandler,
        @Parameter(GroupParameters.Scatter.DataCodec.class) Codec<T> codec,
        @Parameter(GroupParameters.Scatter.ReceiverParams.SelfId.class) String self,
        @Parameter(GroupParameters.Scatter.ReceiverParams.ParentId.class) String parent,
        @Parameter(GroupParameters.Scatter.ReceiverParams.ChildIds.class) String children,
        @Parameter(GroupParameters.IDFactory.class) IdentifierFactory idFac) {
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
    public List<T> receive() throws InterruptedException, NetworkException {
      List<T> result = dataReceiver.receiveList(getParent(), getSelf(),
          Type.Scatter);
      LOG.log(Level.FINEST, getSelf() + " received: " + result + " from "
          + getParent());
      ackSender.send(getSelf(), getParent(), "ACK", Type.Scatter);
      return result;
    }

  }

}
