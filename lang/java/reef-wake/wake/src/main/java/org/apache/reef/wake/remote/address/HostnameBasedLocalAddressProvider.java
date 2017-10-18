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

import javax.inject.Inject;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A LocalAddressProvider that uses <code>Inet4Address.getLocalHost().getHostAddress()</code>.
 */
public final class HostnameBasedLocalAddressProvider implements LocalAddressProvider {

  private static final Logger LOG = Logger.getLogger(HostnameBasedLocalAddressProvider.class.getName());

  private String cached = null;

  /**
   * The constructor is for Tang only.
   */
  @Inject
  private HostnameBasedLocalAddressProvider() {
    LOG.log(Level.FINE, "Instantiating HostnameBasedLocalAddressProvider");
  }

  @Override
  public synchronized String getLocalAddress() {
    if (null == cached) {
      try {
        cached = Inet4Address.getLocalHost().getHostAddress();
      } catch (final UnknownHostException ex) {
        final String message = "Unable to resolve LocalHost. This is fatal.";
        LOG.log(Level.SEVERE, message, ex);
        throw new RuntimeException(message, ex);
      }
    }
    assert null != cached;
    return cached;
  }

  @Override
  public Configuration getConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bind(LocalAddressProvider.class, HostnameBasedLocalAddressProvider.class)
        .build();
  }

  @Override
  public String toString() {
    return "HostnameBasedLocalAddressProvider:" + this.getLocalAddress();
  }
}
