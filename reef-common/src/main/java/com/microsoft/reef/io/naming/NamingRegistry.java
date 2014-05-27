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
package com.microsoft.reef.io.naming;

import com.microsoft.wake.Identifier;

import java.net.InetSocketAddress;

/**
 * Allows to register and unregister addresses for Identifiers.
 */
public interface NamingRegistry {

  /**
   * Register the given Address for the given Identifier
   *
   * @param id
   * @param addr
   * @throws Exception
   */
  public void register(final Identifier id, final InetSocketAddress addr) throws Exception;

  /**
   * Unregister the given Identifier from the registry
   *
   * @param id
   * @throws Exception
   */
  public void unregister(final Identifier id) throws Exception;
}
