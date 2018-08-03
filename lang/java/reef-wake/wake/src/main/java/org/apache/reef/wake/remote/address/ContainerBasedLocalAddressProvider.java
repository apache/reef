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

import org.apache.commons.lang.StringUtils;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A LocalAddressProvider that reads the contents of the file at HOST_IP_ADDR_PATH and uses it to be the ip address.
 */
public final class ContainerBasedLocalAddressProvider implements LocalAddressProvider {

  private static final Pattern IPADDRESS_PATTERN = Pattern.compile(
      "(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)");

  public static final String HOST_IP_ADDR_PATH_ENV = "HOST_IP_ADDR_PATH";
  private static final Logger LOG = Logger.getLogger(ContainerBasedLocalAddressProvider.class.getName());

  private String cachedLocalAddress = null;

  /**
   * The constructor is for Tang only.
   */
  @Inject
  private ContainerBasedLocalAddressProvider() {
    LOG.log(Level.FINE, "Instantiating ContainerBasedLocalAddressProvider");
  }

  @Override
  public synchronized String getLocalAddress() {
    if (cachedLocalAddress != null) {
      return cachedLocalAddress;
    }

    final String ipAddressPath = System.getenv(HOST_IP_ADDR_PATH_ENV);
    LOG.log(Level.FINE, "IpAddressPath is {0}", ipAddressPath);
    if (StringUtils.isEmpty(ipAddressPath)) {
      final String message = String.format("Environment variable must be set for %s", HOST_IP_ADDR_PATH_ENV);
      LOG.log(Level.SEVERE, message);
      throw new RuntimeException(message);
    }

    final File ipAddressFile = new File(ipAddressPath);
    if (!ipAddressFile.exists() || !ipAddressFile.isFile()) {
      final String message = String.format("%s points to invalid path: %s", HOST_IP_ADDR_PATH_ENV, ipAddressPath);
      LOG.log(Level.SEVERE, message);
      throw new RuntimeException(message);
    }

    try {
      cachedLocalAddress = readFile(ipAddressPath, StandardCharsets.UTF_8);
      return cachedLocalAddress;
    } catch (IOException e) {
      final String message = String.format("Exception when attempting to read file %s", ipAddressPath);
      LOG.log(Level.SEVERE, message, e);
      throw new RuntimeException(message, e);
    }
  }

  @Override
  public Configuration getConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bind(LocalAddressProvider.class, ContainerBasedLocalAddressProvider.class)
        .build();
  }

  @Override
  public String toString() {
    return "ContainerBasedLocalAddressProvider:" + this.getLocalAddress();
  }

  private String readFile(final String path, final Charset encoding)
      throws IOException {
    final byte[] encoded = Files.readAllBytes(Paths.get(path));
    final String ipString = new String(encoded, encoding);
    final Matcher matcher = IPADDRESS_PATTERN.matcher(StringUtils.trim(ipString));

    if (!matcher.matches()) {
      throw new RuntimeException(String.format("File at location %s has invalid ip address", path));
    }

    return matcher.group();
  }
}
