/*
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
package org.apache.reef.io.naming;

import org.apache.reef.annotations.audience.Public;
import org.apache.reef.wake.Identifier;

import java.net.InetSocketAddress;

/**
 * Implementations of this interface facilitate address lookups based on
 * Identifiers.
 */
@Public
public interface NamingLookup {

  /**
   * Lookup an Address for a given Identifier.
   *
   * @param id the identifier to be searched
   * @return the Inet address
   * @throws java.io.IOException
   */
  InetSocketAddress lookup(Identifier id) throws Exception;

}
