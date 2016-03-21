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
package org.apache.reef.wake.remote.address;

import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.wake.remote.RemoteConfiguration;

import javax.inject.Inject;
import java.net.InetAddress;

/**
 * A LocalAddressProvider that always uses the Loopback Address. This is used
 * mainly in local runtime for C# to prevent firewall message popups.
 */
public final class LoopbackLocalAddressProvider implements LocalAddressProvider {

  @Inject
  private LoopbackLocalAddressProvider() {
  }

  @Override
  public String getLocalAddress() {
    // Use the loopback address.
    return InetAddress.getLoopbackAddress().getHostAddress();
  }

  @Override
  public Configuration getConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bind(LocalAddressProvider.class, LoopbackLocalAddressProvider.class)
        .bindNamedParameter(RemoteConfiguration.HostAddress.class, getLocalAddress())
        .build();
  }
}
