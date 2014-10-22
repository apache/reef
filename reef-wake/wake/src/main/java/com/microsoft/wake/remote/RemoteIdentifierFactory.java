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
package com.microsoft.wake.remote;


import com.microsoft.tang.annotations.DefaultImplementation;
import com.microsoft.wake.IdentifierFactory;
import com.microsoft.wake.remote.impl.DefaultRemoteIdentifierFactoryImplementation;

/**
 * Factory that creates a RemoteIdentifier
 */
@DefaultImplementation(DefaultRemoteIdentifierFactoryImplementation.class)
public interface RemoteIdentifierFactory extends IdentifierFactory {

  /**
   * Creates a RemoteIdentifier object. This narrows the return type specified
   * in IdentifierFactory.
   * 
   * @param str
   *          a string
   * @return a remote identifier
   */
  @Override
  public RemoteIdentifier getNewInstance(String str);
}
