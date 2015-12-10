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
package org.apache.reef.runtime.yarn.driver;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Default implementation of rack names formatter.
 * Extracts rack name from host name, if possible.
 */
@Private
@DriverSide
public final class DefaultRackNameFormatter implements RackNameFormatter {

  private static final Logger LOG = Logger.getLogger(DefaultRackNameFormatter.class.getName());

  @Inject
  private DefaultRackNameFormatter() {
  }

  /**
   * @see RackNameFormatter#getRackName(Container)
   */
  @Override
  public String getRackName(final Container container) {
    final String hostName = container.getNodeId().getHost();

    // the rack name comes as part of the host name, e.g.
    // <rackName>-<hostNumber>
    // we perform some checks just in case it doesn't
    String rackName = null;
    if (hostName != null) {
      final String[] rackNameAndNumber = hostName.split("-");
      if (rackNameAndNumber.length == 2) {
        rackName = rackNameAndNumber[0];
      } else {
        LOG.log(Level.WARNING, "Could not get information from the rack name, should use the default");
      }
    }

    return rackName;
  }
}
