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
package org.apache.reef.io.network.util;

import org.apache.reef.io.network.NetworkServiceParameter;
import org.apache.reef.io.network.naming.NameServerParameters;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;

import javax.inject.Inject;

/**
 * Easily provide driver id
 */
public final class DriverIDProvider {

  private final Identifier driverId;

  @Inject
  public DriverIDProvider(
      @Parameter(NameServerParameters.NameServerIdentifierFactory.class) IdentifierFactory factory,
      @Parameter(NetworkServiceParameter.DriverNetworkServiceIdentifier.class) String driverId) {

    this.driverId = factory.getNewInstance(driverId);
  }

  public Identifier getId() {
    return driverId;
  }

  @Override
  public String toString() {
    return driverId.toString();
  }
}
