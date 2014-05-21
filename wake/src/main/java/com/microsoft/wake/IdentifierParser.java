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
