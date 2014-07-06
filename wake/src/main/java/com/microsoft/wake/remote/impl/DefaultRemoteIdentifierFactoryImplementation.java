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
package com.microsoft.wake.remote.impl;

import java.util.Map;

import javax.inject.Inject;


import com.microsoft.wake.Identifier;
import com.microsoft.wake.impl.DefaultIdentifierFactory;
import com.microsoft.wake.remote.RemoteIdentifier;
import com.microsoft.wake.remote.RemoteIdentifierFactory;

public class DefaultRemoteIdentifierFactoryImplementation extends DefaultIdentifierFactory
    implements RemoteIdentifierFactory {

  @Inject
  public DefaultRemoteIdentifierFactoryImplementation() {
    super();
  }
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public DefaultRemoteIdentifierFactoryImplementation(Map<String, Class<? extends RemoteIdentifier>> typeToClazzMap) {
    super((Map<String, Class<? extends Identifier>>)(Map)typeToClazzMap);
  }
  
  @Override
  public RemoteIdentifier getNewInstance(String str) {
    return (RemoteIdentifier)super.getNewInstance(str);
  }

}
