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
package com.microsoft.reef.io.network.naming;

import com.microsoft.reef.io.naming.NameAssignment;
import com.microsoft.reef.io.network.naming.serialization.*;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.Identifier;
import com.microsoft.wake.IdentifierFactory;
import com.microsoft.wake.Stage;
import com.microsoft.wake.impl.MultiEventHandler;
import com.microsoft.wake.impl.SyncStage;
import com.microsoft.wake.remote.Codec;
import com.microsoft.wake.remote.NetUtils;
import com.microsoft.wake.remote.impl.TransportEvent;
import com.microsoft.wake.remote.transport.Transport;
import com.microsoft.wake.remote.transport.netty.NettyMessagingTransport;

import javax.inject.Inject;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Naming server
 */
public class NameServer implements Stage {
  private static final Logger LOG = Logger.getLogger(NameServer.class.getName());

  private final Transport transport;
  private final Map<Identifier, InetSocketAddress> idToAddrMap;
  
  private final int port;
  
  /**
   * Constructs a name server
   * 
   * @param port a listening port number
   * @param factory an identifier factory
   */
  @Inject
  public NameServer(@Parameter(NameServerParameters.NameServerPort.class) int port, 
      @Parameter(NameServerParameters.NameServerIdentifierFactory.class) IdentifierFactory factory) {
    
    Codec<NamingMessage> codec = NamingCodecFactory.createFullCodec(factory);
    EventHandler<NamingMessage> handler = createEventHandler(codec);
    
    transport = new NettyMessagingTransport(NetUtils.getLocalAddress(), port, null, new SyncStage<TransportEvent>(new NamingServerHandler(handler, codec)));
    this.port = transport.getListeningPort();
    idToAddrMap = Collections.synchronizedMap(new HashMap<Identifier, InetSocketAddress> ());
    
    LOG.log(Level.INFO, "NameServer starting, listening at port " + this.port);
  }

  private EventHandler<NamingMessage> createEventHandler(Codec<NamingMessage> codec) {
    Map<Class<? extends NamingMessage>, EventHandler<? extends NamingMessage>> clazzToHandlerMap 
      = new HashMap<Class<? extends NamingMessage>, EventHandler<? extends NamingMessage>> ();
    clazzToHandlerMap.put(NamingLookupRequest.class, new NamingLookupRequestHandler(this, codec));
    clazzToHandlerMap.put(NamingRegisterRequest.class, new NamingRegisterRequestHandler(this, codec));
    clazzToHandlerMap.put(NamingUnregisterRequest.class, new NamingUnregisterRequestHandler(this));
    EventHandler<NamingMessage> handler = new MultiEventHandler<NamingMessage>(clazzToHandlerMap);
    return handler;
  }
  
  /**
   * Gets port
   */
  public int getPort() {
	return port;
  }
  
  /**
   * Closes resources
   */
  @Override
  public void close() throws Exception {
    transport.close();    
  }

  /**
   * Registers an (identifier, address) mapping locally
   * 
   * @param id an identifier
   * @param addr an Internet socket address
   */
  public void register(Identifier id, InetSocketAddress addr) {
    LOG.log(Level.FINE, "id: " + id + " addr: " + addr);
    idToAddrMap.put(id, addr);
  }

  /**
   * Unregisters an identifier locally
   * 
   * @param id an identifier
   */
  public void unregister(Identifier id) {
    LOG.log(Level.FINE, "id: " + id);
    idToAddrMap.remove(id);    
  }

  /**
   * Finds an address for an identifier locally
   * 
   * @param id an identifier
   * @return an Internet socket address
   */
  public InetSocketAddress lookup(Identifier id) {
    LOG.log(Level.FINE, "id: " + id);
    return idToAddrMap.get(id);
  }

  /**
   * Finds addresses for identifiers locally
   * 
   * @param identifiers an iterable of identifiers
   * @return a list of name assignments
   */
  public List<NameAssignment> lookup(Iterable<Identifier> identifiers) {
    LOG.log(Level.FINE, "identifiers");
    List<NameAssignment> nas = new ArrayList<NameAssignment> ();
    for(Identifier id : identifiers) {
      InetSocketAddress addr = idToAddrMap.get(id);
      LOG.log(Level.FINEST, "id : " + id + " addr: " + addr);        	

      if (addr != null)
        nas.add(new NameAssignmentTuple(id, addr));
    }
    return nas;
  }
  
}

/**
 * Naming server transport event handler that invokes a specific naming message handler
 */
class NamingServerHandler implements EventHandler<TransportEvent> {

  private final Codec<NamingMessage> codec;
  private final EventHandler<NamingMessage> handler;

  NamingServerHandler(EventHandler<NamingMessage> handler, Codec<NamingMessage> codec) {
    this.codec = codec;
    this.handler = handler;
  }
  
  @Override
  public void onNext(TransportEvent value) {
    byte[] data = value.getData();
    NamingMessage message = (NamingMessage)codec.decode(data);
    message.setLink(value.getLink());
    handler.onNext(message);
  }
  
}

/**
 * Naming lookup request handler
 */
class NamingLookupRequestHandler implements EventHandler<NamingLookupRequest> {

  private final NameServer server;
  private final Codec<NamingMessage> codec;
  
  public NamingLookupRequestHandler(NameServer server, Codec<NamingMessage> codec) {
    this.server = server;
    this.codec = codec;
  }
  
  @Override
  public void onNext(NamingLookupRequest value) {
    List<NameAssignment> nas = server.lookup(value.getIdentifiers());
    byte[] resp = codec.encode(new NamingLookupResponse(nas));
    try {
      value.getLink().write(resp);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
}

/**
 * Naming register request handler
 */
class NamingRegisterRequestHandler implements EventHandler<NamingRegisterRequest> {

  private final NameServer server;
  private final Codec<NamingMessage> codec;
  
  public NamingRegisterRequestHandler(NameServer server, Codec<NamingMessage> codec) {
    this.server = server;
    this.codec = codec;
  }
  
  @Override
  public void onNext(NamingRegisterRequest value) {
    server.register(value.getNameAssignment().getIdentifier(), value.getNameAssignment().getAddress());
    byte[] resp = codec.encode(new NamingRegisterResponse(value));
    try {
      value.getLink().write(resp);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
}

/**
 * Naming unregister request handler
 */
class NamingUnregisterRequestHandler implements EventHandler<NamingUnregisterRequest> {

  private final NameServer server;
  
  public NamingUnregisterRequestHandler(NameServer server) {
    this.server = server;
  }
  
  @Override
  public void onNext(NamingUnregisterRequest value) {
    server.unregister(value.getIdentifier());
  }
  
}


