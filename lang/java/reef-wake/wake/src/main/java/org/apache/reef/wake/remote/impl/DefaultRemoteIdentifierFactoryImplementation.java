/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.reef.wake.remote.impl;

import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.impl.DefaultIdentifierFactory;
import org.apache.reef.wake.remote.RemoteIdentifier;
import org.apache.reef.wake.remote.RemoteIdentifierFactory;

import javax.inject.Inject;
import java.util.Map;

public class DefaultRemoteIdentifierFactoryImplementation extends DefaultIdentifierFactory
    implements RemoteIdentifierFactory {

  @Inject
  public DefaultRemoteIdentifierFactoryImplementation() {
    super();
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public DefaultRemoteIdentifierFactoryImplementation(Map<String, Class<? extends RemoteIdentifier>> typeToClazzMap) {
    super((Map<String, Class<? extends Identifier>>) (Map) typeToClazzMap);
  }

  @Override
  public RemoteIdentifier getNewInstance(String str) {
    return (RemoteIdentifier) super.getNewInstance(str);
  }

}
