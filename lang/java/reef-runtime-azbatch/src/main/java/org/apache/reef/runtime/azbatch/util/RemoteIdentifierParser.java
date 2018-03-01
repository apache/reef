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
package org.apache.reef.runtime.azbatch.util;

/**
 * Utility class to parse IP and port information from Azure Batch Node ID.
 */
public final class RemoteIdentifierParser {

  private static final String PROTOCOL = "socket://";

  private RemoteIdentifierParser() {}

  /**
   * Get the IP address from the remote identifier.
   *
   * @param remoteIdentifier the remote identifier.
   * @return the IP address.
   */
  public static String parseIp(final String remoteIdentifier) {
    return remoteIdentifier.substring(PROTOCOL.length(), remoteIdentifier.lastIndexOf(':'));
  }

  /**
   * Get the port from the remote identifier.
   *
   * @param remoteIdentifier the remote identifier.
   * @return the port.
   */
  public static int parsePort(final String remoteIdentifier) {
    return Integer.parseInt(remoteIdentifier.substring(remoteIdentifier.lastIndexOf(':') + 1));
  }

  /**
   * Get the node ID from the remote identifier.
   *
   * @param remoteIdentifier the remote identifier.
   * @return the node ID.
   */
  public static String parseNodeId(final String remoteIdentifier) {
    return remoteIdentifier.substring(PROTOCOL.length());
  }
}
