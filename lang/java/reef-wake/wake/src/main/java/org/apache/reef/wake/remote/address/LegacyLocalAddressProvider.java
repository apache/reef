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
import org.apache.reef.wake.exception.WakeRuntimeException;

import javax.inject.Inject;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An implementation of LocalAddressProvider using the (removed) code from NetUtils.getLocalAddress().
 * @deprecated in 0.14. Use HostnameBasedLocalAddressProvider instead
 */
@Deprecated
public final class LegacyLocalAddressProvider implements LocalAddressProvider {
  private static final Logger LOG = Logger.getLogger(LegacyLocalAddressProvider.class.getName());
  private final AtomicReference<String> cached = new AtomicReference<>();

  /**
   * Injectable constructor for Tang only.
   */
  @Inject
  private LegacyLocalAddressProvider() {
    LOG.log(Level.FINE, "Instantiating LegacyLocalAddressProvider");
  }

  @Override
  public String getLocalAddress() {
    // This method is surprisingly slow: It was causing unit test timeouts, so we memoize the result.
    if (cached.get() == null) {
      final Enumeration<NetworkInterface> ifaces;
      try {
        ifaces = NetworkInterface.getNetworkInterfaces();
        final TreeSet<Inet4Address> sortedAddrs = new TreeSet<>(new AddressComparator());
        // There is an idea of virtual / subinterfaces exposed by java here.
        // We're not walking around looking for those because the javadoc says:

        // "NOTE: can use getNetworkInterfaces()+getInetAddresses() to obtain all IP addresses for this node"

        while (ifaces.hasMoreElements()) {
          final NetworkInterface iface = ifaces.nextElement();
//          if(iface.isUp()) {  // leads to slowness and non-deterministic return values, so don't call isUp().
          final Enumeration<InetAddress> addrs = iface.getInetAddresses();
          while (addrs.hasMoreElements()) {
            final InetAddress a = addrs.nextElement();
            if (a instanceof Inet4Address) {
              sortedAddrs.add((Inet4Address) a);
            }
//            }
          }
        }
        if (sortedAddrs.isEmpty()) {
          throw new WakeRuntimeException("This machine apparently doesn't have any IP addresses (not even 127.0.0.1) " +
              "on interfaces that are up.");
        }
        cached.set(sortedAddrs.pollFirst().getHostAddress());
        LOG.log(Level.FINE, "Local address is {0}", cached.get());
      } catch (final SocketException e) {
        throw new WakeRuntimeException("Unable to get local host address", e);
      }
    }
    return cached.get();
  }

  @Override
  public Configuration getConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bind(LocalAddressProvider.class, LegacyLocalAddressProvider.class)
        .build();
  }

  private static class AddressComparator implements Comparator<Inet4Address> {

    // get unsigned byte.
    private static int u(final byte b) {
      return (int) b; // & 0xff;
    }

    @Override
    public int compare(final Inet4Address aa, final Inet4Address ba) {
      final byte[] a = aa.getAddress();
      final byte[] b = ba.getAddress();
      // local subnet comes after all else.
      if (a[0] == 127 && b[0] != 127) {
        return 1;
      }
      if (a[0] != 127 && b[0] == 127) {
        return -1;
      }
      for (int i = 0; i < 4; i++) {
        if (u(a[i]) < u(b[i])) {
          return -1;
        }
        if (u(a[i]) > u(b[i])) {
          return 1;
        }
      }
      return 0;
    }
  }
}
