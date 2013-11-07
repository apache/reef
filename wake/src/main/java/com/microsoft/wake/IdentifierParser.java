package com.microsoft.wake;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.inject.Inject;


import com.microsoft.tang.ExternalConstructor;
import com.microsoft.wake.impl.DefaultIdentifierFactory;
import com.microsoft.wake.remote.impl.SocketRemoteIdentifier;
import com.microsoft.wake.storage.FileIdentifier;

public class IdentifierParser implements ExternalConstructor<Identifier> {
  private static final IdentifierFactory factory;
  
  // TODO: Modify tang to allow this to use a factory pattern.
  static {
    Map<String, Class<? extends Identifier>> map = new ConcurrentHashMap<>();

    map.put("socket", SocketRemoteIdentifier.class);
    map.put("file", FileIdentifier.class);
    
    factory = new DefaultIdentifierFactory(map);
  }
  final Identifier id; 
  @Inject
  IdentifierParser(String s) {
    id = factory.getNewInstance(s);
  }
  
  @Override
  public Identifier newInstance() {
    return id;
  }

}
