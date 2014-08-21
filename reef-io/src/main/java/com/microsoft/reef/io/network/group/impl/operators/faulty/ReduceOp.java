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
package com.microsoft.reef.io.network.group.impl.operators.faulty;

import com.microsoft.reef.exception.evaluator.NetworkException;
import com.microsoft.reef.io.network.Connection;
import com.microsoft.reef.io.network.group.operators.Reduce.ReduceFunction;
import com.microsoft.reef.io.network.impl.NetworkService;
import com.microsoft.reef.io.network.impl.NetworkServiceParameters;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type;
import com.microsoft.reef.io.network.util.Utils;
import com.microsoft.reef.util.Optional;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.Identifier;
import com.microsoft.wake.IdentifierFactory;
import com.microsoft.wake.remote.Codec;

import javax.inject.Inject;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ReduceOp {
  private static final Logger LOG = Logger.getLogger(ReduceOp.class.getName());

  public static class Sender<V> {
    private final Identifier self;
    private final Identifier parent;
    private final Set<Identifier> children = new HashSet<>();
    private final Codec<V> codec;
    private final ReduceFunction<V> redFunc;
    private final ReduceHandler handler;
    private final NetworkService<GroupCommMessage> netService;
    private final ExceptionHandler excHandler;
    private final boolean reusePreviousValues;
    private final Map<Identifier, V> previousValues;

    @Inject
    public Sender(final NetworkService<GroupCommMessage> netService,
                  final ReduceHandler handler,
                  final @Parameter(BroadReduceConfig.ReduceConfig.DataCodec.class) Codec<V> codec,
                  final @Parameter(BroadReduceConfig.ReduceConfig.ReduceFunction.class) ReduceFunction<V> redFunc,
                  final @Parameter(BroadReduceConfig.ReduceConfig.Sender.SelfId.class) String selfId,
                  final @Parameter(BroadReduceConfig.ReduceConfig.Sender.ParentId.class) String parentId,
                  final @Parameter(BroadReduceConfig.ReduceConfig.Sender.ChildIds.class) Set<String> childIds,
                  final @Parameter(BroadReduceConfig.ReduceConfig.Sender.ApproximateGradient.class) boolean reusePreviousValues,
                  final @Parameter(BroadReduceConfig.IdFactory.class) IdentifierFactory idFac,
                  final @Parameter(NetworkServiceParameters.NetworkServiceExceptionHandler.class) EventHandler<Exception> excHandler) {
      this.netService = netService;
      this.handler = handler;
      this.codec = codec;
      this.redFunc = redFunc;
      this.parent = (parentId.equals(BroadReduceConfig.defaultValue)) ? null : idFac.getNewInstance(parentId);
      this.reusePreviousValues = reusePreviousValues;
      if (this.reusePreviousValues) {
        previousValues = new HashMap<>();
      } else {
        previousValues = null;
      }
      this.self = (selfId.equals(BroadReduceConfig.defaultValue) ? null : idFac.getNewInstance(selfId));
      this.excHandler = (ExceptionHandler) excHandler;

      LOG.log(Level.FINEST, "Approximate Gradient: " + reusePreviousValues);
      LOG.log(Level.FINEST, "Received childIds:");
      for (final String childId : childIds) {
        LOG.log(Level.FINEST, childId);
        if (!childId.equals(AllReduceConfig.defaultValue)) {
          this.children.add(idFac.getNewInstance(childId));
        }
      }
    }

    public void sync() {
      final Map<Identifier, Integer> isIdAlive = new HashMap<>();
      this.handler.sync(isIdAlive);
      SyncHelper.update(this.children, isIdAlive, this.self);
    }

    /**
     * @param myData
     * @throws InterruptedException
     * @throws NetworkException
     */
    public void send(final V myData) throws NetworkFault, NetworkException, InterruptedException {
      LOG.log(Level.FINEST, "I am Reduce sender" + self.toString());
      final List<V> vals = new ArrayList<>(this.children.size() + 1);
      vals.add(myData);
      for (final Identifier child : children) {
        LOG.log(Level.FINEST, "Waiting for child: " + child);
        final Optional<V> valueFromChild = getValueForChild(child);
        if (valueFromChild.isPresent()) {
          vals.add(valueFromChild.get());
        }
      }

      //Reduce the received values
      final V reducedValue = redFunc.apply(vals);
      LOG.log(Level.FINEST, "Sending " + reducedValue + " to parent: " + parent);
      assert (parent != null);
      this.send(reducedValue, parent);
    }

    /**
     * Fetches the value from the given child.
     * If a value is received it is returned and stored as the last gradient if we do approximate gradients.
     * <p/>
     * If no value is received and we do approximate gradients, the last known gradient is returned instead (if any).
     * <p/>
     * If no value is received and we do not approximate gradients, no value is returned.
     *
     * @param childIdentifier
     * @return
     * @throws NetworkException
     * @throws InterruptedException
     */
    private Optional<V> getValueForChild(final Identifier childIdentifier) throws NetworkException, InterruptedException {
      LOG.log(Level.FINEST, "Waiting for child: " + childIdentifier);
      final V valueFromChild = handler.get(childIdentifier, codec);
      LOG.log(Level.FINEST, "Received: " + valueFromChild);

      final Optional<V> returnValue;
      if (valueFromChild != null) {
        // Use this gradient and update the last gradient seen.
        returnValue = Optional.of(valueFromChild);
        if (this.reusePreviousValues) {
          previousValues.put(childIdentifier, valueFromChild);
        }
      } else {
        // Use the last gradient seen or nothing.
        if (this.reusePreviousValues && this.previousValues.containsKey(childIdentifier)) {
          returnValue = Optional.of(previousValues.get(childIdentifier));
        } else {
          returnValue = Optional.empty();
        }
      }
      return returnValue;
    }

    /**
     * Sends the given value to the given destination
     *
     * @param value
     * @param destination
     * @throws NetworkException
     */
    private void send(final V value, final Identifier destination) throws NetworkException {
      if (excHandler.hasExceptions())
        throw new NetworkException("Unable to send msgs");
      final Connection<GroupCommMessage> link = netService.newConnection(destination);
      link.open();
      link.write(Utils.bldGCM(Type.Reduce, self, destination, codec.encode(value)));
    }
  }

  public static class Receiver<V> {
    private final Identifier self;
    private final Set<Identifier> children = new HashSet<>();
    private final Codec<V> codec;
    private final ReduceFunction<V> redFunc;
    private final ReduceHandler handler;

    @Inject
    public Receiver(final NetworkService<GroupCommMessage> netService,
                    final ReduceHandler handler,
                    final @Parameter(BroadReduceConfig.ReduceConfig.DataCodec.class) Codec<V> codec,
                    @Parameter(BroadReduceConfig.ReduceConfig.ReduceFunction.class) ReduceFunction<V> redFunc,
                    @Parameter(BroadReduceConfig.ReduceConfig.Receiver.SelfId.class) String selfId,
                    @Parameter(BroadReduceConfig.ReduceConfig.Receiver.ParentId.class) String parentId,
                    @Parameter(BroadReduceConfig.ReduceConfig.Receiver.ChildIds.class) Set<String> childIds,
                    @Parameter(BroadReduceConfig.IdFactory.class) IdentifierFactory idFac) {
      this.handler = handler;
      this.codec = codec;
      this.redFunc = redFunc;
      this.self = (selfId.equals(BroadReduceConfig.defaultValue) ? null : idFac.getNewInstance(selfId));

      LOG.log(Level.FINEST, "Received childIds: " + childIds);
      for (final String childId : childIds) {
        if (!childId.equals(AllReduceConfig.defaultValue)) {
          children.add(idFac.getNewInstance(childId));
        }
      }
    }

    public void sync() {
      final Map<Identifier, Integer> isIdAlive = new HashMap<>();
      this.handler.sync(isIdAlive);
      SyncHelper.update(this.children, isIdAlive, this.self);
    }

    public V reduce() throws NetworkException, InterruptedException {
      //I am root.
      LOG.log(Level.FINEST, "I am root " + self);
      //Wait for children to send
      final List<V> vals = new ArrayList<>(this.children.size());

      for (final Identifier childIdentifier : this.children) {
        LOG.log(Level.FINEST, "Waiting for child: " + childIdentifier);
        final V cVal = handler.get(childIdentifier, codec);
        LOG.log(Level.FINEST, "Received: " + cVal);
        if (cVal != null) {
          vals.add(cVal);
        }
      }

      //Reduce the received values
      final V redVal = redFunc.apply(vals);
      LOG.log(Level.FINEST, "Local Reduced value: " + redVal);
      return redVal;
    }

  }
}
