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
import java.io.File;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A LocalAddressProvider that uses <code>Inet4Address.getLocalHost().getHostAddress()</code>.
 */
public final class ContainerBasedLocalAddressProvider implements LocalAddressProvider {

  private static final Logger LOG = Logger.getLogger(ContainerBasedLocalAddressProvider.class.getName());

  private String cached = null;

  /**
   * The constructor is for Tang only.
   */
  @Inject
  private ContainerBasedLocalAddressProvider() {
    LOG.log(Level.FINE, "Instantiating ContainerBasedLocalAddressProvider");
  }

  @Override
  public synchronized String getLocalAddress() {
    if (null == cached) {
      String ipAddressPath = System.getenv("HOST_IP_ADDR_PATH");
      LOG.log(Level.INFO, "IpAddressPath is {0}", ipAddressPath);
      if (ipAddressPath != null) {
        File ipAddressFile = new File(ipAddressPath);
        if (ipAddressFile.exists() && ipAddressFile.isFile()) {
          String filePath = expandEnvironmentVariables(ipAddressPath);
          try {
            cached = readFile(filePath, StandardCharsets.UTF_8);
          } catch (IOException e) {
            String message = String.format("Exception when attempting to read file %s", filePath);
            LOG.log(Level.SEVERE, message, e);
            throw new RuntimeException(message);
          }
        } else {
          final String message = "HOST_IP_ADDR_PATH points to invalid path.";
          LOG.log(Level.SEVERE, message);
          throw new RuntimeException(message);
        }
      } else {
        final String message = "Environment variable must be set for HOST_IP_ADDR_PATH";
        LOG.log(Level.SEVERE, message);
        throw new RuntimeException(message);
      }
    }
    assert null != cached;
    LOG.log(Level.INFO, "Returning ContainerBasedLocalAddressProvider.getLocalAddress() as " + cached);
    return cached;
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

  public static String expandEnvironmentVariables(String text) {
    Map<String, String> envMap = System.getenv();
    for (Map.Entry<String, String> entry : envMap.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      text = text.replaceAll("%" + key + "%", value);
    }
    return text;
  }

  static String readFile(String path, Charset encoding)
      throws IOException
  {
    byte[] encoded = Files.readAllBytes(Paths.get(path));
    return new String(encoded, encoding);
  }
}
