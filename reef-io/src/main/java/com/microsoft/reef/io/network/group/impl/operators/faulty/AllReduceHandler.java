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

import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import javax.inject.Inject;

import com.microsoft.reef.exception.evaluator.NetworkException;
import com.microsoft.reef.io.network.Message;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupMessageBody;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.Identifier;
import com.microsoft.wake.IdentifierFactory;
import com.microsoft.wake.remote.Codec;

/**
 * 
 */
public class AllReduceHandler implements EventHandler<Message<GroupCommMessage>>{
  
  private final ConcurrentHashMap<Identifier, BlockingQueue<GroupCommMessage>> id2que = new ConcurrentHashMap<>();
  
  @NamedParameter(doc = "List of Identifiers on which the handler should listen")
  public static class IDs implements Name<Set<String>> {  }
  
  private final IdentifierFactory idFac;
  
  @Inject
  public AllReduceHandler(@Parameter(IDs.class) Set<String> ids,
      @Parameter(AllReduceConfig.IdFactory.class) IdentifierFactory idFac) {
    this.idFac = idFac;
    System.out.println("\t\tI can listen from:");
    for(String id : ids){
      Identifier compId = idFac.getNewInstance(id);
      id2que.put(compId, new LinkedBlockingQueue<GroupCommMessage>());
      System.out.println("\t\t" + compId);
    }
  }

  @Override
  public void onNext(Message<GroupCommMessage> value) {
    GroupCommMessage oneVal = null;
    if(value.getData().iterator().hasNext())
      oneVal = value.getData().iterator().next();

    Identifier srcId = idFac.getNewInstance(oneVal.getSrcid());
    try {
      System.out.println("\t\tonNext( " + oneVal +") from: " + srcId);
      id2que.get(srcId).put(oneVal);
    } catch (InterruptedException e) {
      throw new RuntimeException("Could not put " + oneVal + " into the queue of " + srcId, e);
    }
  } 

  /**
   * @param child
   * @return
   * @throws InterruptedException 
   * @throws NetworkException 
   */
  public <T> T get(Identifier id, Codec<T> codec) throws InterruptedException, NetworkException {
    System.out.println("\t\tget from " + id);
    if(!id2que.containsKey(id)){
      System.out.println("\t\tCan't receive from a non-child");
      throw new RuntimeException("Can't receive from a non-child");
    }
    T retVal = null;
    GroupCommMessage gcm = id2que.get(id).take();
    if(gcm.getType()==Type.SourceDead){
      System.out.println("\t\tGot src dead msg from driver. Terminating wait and returning null");
      return null;
    }
    for(GroupMessageBody body : gcm.getMsgsList()){
      retVal = codec.decode(body.getData().toByteArray());
    }
    System.out.println("\t\tReturning " + retVal);
    return retVal;
  }

}
