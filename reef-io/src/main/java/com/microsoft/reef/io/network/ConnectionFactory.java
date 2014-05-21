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
package com.microsoft.reef.io.network;

import com.microsoft.wake.Identifier;

/**
 * Factory that creates a new connection 
 *
 * @param <T> type
 */
public interface ConnectionFactory<T> {
  
  /**
   * Creates a new connection
   * 
   * @param destId a destination identifier
   * @return a connection
   */
  public Connection<T> newConnection(Identifier destId);
}
