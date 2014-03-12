/*
 * Copyright 2013 Microsoft.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.io.network.group.impl.operators.faulty;

import com.microsoft.reef.exception.evaluator.NetworkException;
import com.microsoft.reef.io.network.Message;
import com.microsoft.reef.io.network.group.impl.operators.faulty.BroadRedHandler.IDs;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupMessageBody;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.Identifier;
import com.microsoft.wake.IdentifierFactory;
import com.microsoft.wake.remote.Codec;

import javax.inject.Inject;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 */
public class BroadcastHandler implements EventHandler<Message<GroupCommMessage>> {
  private static final Logger LOG = Logger.getLogger(BroadcastHandler.class.getName());

  private final ConcurrentHashMap<Identifier, BlockingQueue<GroupCommMessage>> id2dataQue = new ConcurrentHashMap<>();
  private final BlockingQueue<GroupCommMessage> ctrlQue = new LinkedBlockingQueue<>();

  private final IdentifierFactory idFac;

  @Inject
  public BroadcastHandler(@Parameter(IDs.class) Set<String> ids,
                          @Parameter(BroadReduceConfig.IdFactory.class) IdentifierFactory idFac) {
    this.idFac = idFac;
    LOG.log(Level.FINEST, "\t\tI can listen from:");
    for (String id : ids) {
      Identifier compId = idFac.getNewInstance(id);
      addChild(compId);
      LOG.log(Level.FINEST, "\t\t" + compId);
    }
  }

  public synchronized void addChild(Identifier compId) {
    LOG.log(Level.FINEST, "Adding " + compId + " as one of the senders to which I can listen from");
    id2dataQue.put(compId, new LinkedBlockingQueue<GroupCommMessage>());
  }

  public synchronized void removeChild(Identifier compId) {
    LOG.log(Level.FINEST, "Removing " + compId + " as one of the senders to which I can listen from");
    id2dataQue.remove(compId);
  }

  @Override
  public void onNext(Message<GroupCommMessage> value) {
    GroupCommMessage oneVal = null;
    if (value.getData().iterator().hasNext())
      oneVal = value.getData().iterator().next();
    Identifier srcId = idFac.getNewInstance(oneVal.getSrcid());
    try {
      LOG.log(Level.FINEST, "\t\t" + oneVal.getType() + " from:" + srcId);
      LOG.info(oneVal.getType() + " from:" + srcId);
      if (Type.SourceAdd == oneVal.getType()) {
        ctrlQue.put(oneVal);
      } else if (Type.SourceDead == oneVal.getType()) {
        ctrlQue.put(oneVal);
        if (id2dataQue.containsKey(srcId))
          id2dataQue.get(srcId).put(oneVal);
      } else {
        if (!id2dataQue.containsKey(srcId)) {
          LOG.log(Level.FINEST, "Ignoring msg as I am not configured to recv from " + srcId);
          return;
        }
        id2dataQue.get(srcId).add(oneVal);
      }
    } catch (InterruptedException e) {
      throw new RuntimeException("Could not put " + oneVal + " into the queue of " + srcId, e);
    }
  }

  public void sync(Map<Identifier, Integer> isIdAlive/*Set<Identifier> born, Set<Identifier> dead*/) {
    LOG.log(Level.FINEST, "Synching any control messages");
    while (!ctrlQue.isEmpty()) {
      GroupCommMessage gcm = ctrlQue.poll();
      Identifier id = idFac.getNewInstance(gcm.getSrcid());
      if (gcm.getType() == Type.SourceAdd) {
        int status = 0;
        if (isIdAlive.containsKey(id))
          status = isIdAlive.get(id);
        isIdAlive.put(id, status + 1);
      } else {
        int status = 0;
        if (isIdAlive.containsKey(id))
          status = isIdAlive.get(id);
        isIdAlive.put(id, status - 1);
      }
    }
    LOG.log(Level.FINEST, "Id to life status: " + isIdAlive);

    for (Identifier identifier : isIdAlive.keySet()) {
      int status = isIdAlive.get(identifier);
      if (status < 0) {
        LOG.log(Level.FINEST, identifier + " is dead(" + status + "). Removing from handler");
        removeChild(identifier);
      } else if (status > 0) {
        LOG.log(Level.FINEST, identifier + " is alive(" + status + "). Adding to handler");
        addChild(identifier);
      } else {
        //status == 0
        //if(handler can receive from this id)
        //  means that (srcDead + srcAdd)*
        //  TODO: This might put some src dead 
        //  msgs into the queue. We need to remove
        //  those additional srcDeads
        //else
        //  means that (srcAdd + srcDead)*
        //  srcDead msgs will be ignored
        //  So no problem here
      }
    }
  }

  /**
   * @param child
   * @return
   * @throws InterruptedException
   * @throws NetworkException
   */
  public <T> T get(Identifier id, Codec<T> codec) throws InterruptedException, NetworkException {
    LOG.log(Level.FINEST, "\t\tget from " + id);
    LOG.info("get from " + id);

    if (!id2dataQue.containsKey(id)) {
      LOG.log(Level.FINEST, "\t\tCan't receive from a non-child");
      throw new RuntimeException("Can't receive from a non-child");
    }
    T retVal = null;
    GroupCommMessage gcm = id2dataQue.get(id).take();

    if (gcm.getType() == Type.SourceDead) {
      LOG.log(Level.FINEST, "\t\tGot src dead msg from driver. Terminating wait and returning null");
      return null;
    }
    for (GroupMessageBody body : gcm.getMsgsList()) {
      retVal = codec.decode(body.getData().toByteArray());
    }
    LOG.info("Returning " + retVal);
    return retVal;
  }
}
