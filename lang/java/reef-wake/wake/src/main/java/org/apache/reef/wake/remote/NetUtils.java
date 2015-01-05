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
package org.apache.reef.wake.remote;

import org.apache.reef.wake.exception.WakeRuntimeException;

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

public class NetUtils {
  private static final Logger LOG = Logger.getLogger(NetUtils.class.getName());

  private static AtomicReference<String> cached = new AtomicReference<>();

  public static String getLocalAddress() {
    // This method is surprisingly slow: It was causing unit test timeouts, so we memoize the result.
    if (cached.get() == null) {
      Enumeration<NetworkInterface> ifaces;
      try {
        ifaces = NetworkInterface.getNetworkInterfaces();
        TreeSet<Inet4Address> sortedAddrs = new TreeSet<>(new AddressComparator());
        // There is an idea of virtual / subinterfaces exposed by java here.
        // We're not walking around looking for those because the javadoc says:

        // "NOTE: can use getNetworkInterfaces()+getInetAddresses() to obtain all IP addresses for this node"

        while (ifaces.hasMoreElements()) {
          NetworkInterface iface = ifaces.nextElement();
//          if(iface.isUp()) {  // leads to slowness and non-deterministic return values, so don't call isUp().
          Enumeration<InetAddress> addrs = iface.getInetAddresses();
          while (addrs.hasMoreElements()) {
            InetAddress a = addrs.nextElement();
            if (a instanceof Inet4Address) {
              sortedAddrs.add((Inet4Address) a);
            }
//            }
          }
        }
        if (sortedAddrs.isEmpty()) {
          throw new WakeRuntimeException("This machine apparently doesn't have any IP addresses (not even 127.0.0.1) on interfaces that are up.");
        }
        cached.set(sortedAddrs.pollFirst().getHostAddress());
        LOG.log(Level.FINE, "Local address is {0}", cached.get());
      } catch (SocketException e) {
        throw new WakeRuntimeException("Unable to get local host address",
            e.getCause());
      }
    }
    return cached.get();
  }

  private static class AddressComparator implements Comparator<Inet4Address> {

    // get unsigned byte.
    private static int u(byte b) {
      return ((int) b);// & 0xff;
    }

    @Override
    public int compare(Inet4Address aa, Inet4Address ba) {
      byte[] a = aa.getAddress();
      byte[] b = ba.getAddress();
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
