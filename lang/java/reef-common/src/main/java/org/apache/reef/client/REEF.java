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
package org.apache.reef.client;

import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.annotations.audience.Public;
import org.apache.reef.runtime.common.client.REEFImplementation;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * The main entry point into the REEF resourcemanager.
 * <p>
 * Every REEF resourcemanager provides an implementation of this interface. That
 * instance is used to submitTask the Driver class for execution to REEF. As with
 * all submissions in REEF, this is done in the form of a TANG Configuration
 * object.
 */
@Public
@ClientSide
@DefaultImplementation(REEFImplementation.class)
public interface REEF extends AutoCloseable {

  /**
   * Close the resourcemanager connection.
   */
  @Override
  void close();

  /**
   * Submits the Driver set up in the given Configuration for execution.
   * <p>
   * The Configuration needs to bind the Driver interface to an actual
   * implementation of that interface for the job at hand.
   *
   * @param driverConf The driver configuration: including everything it needs to execute.  @see DriverConfiguration
   */
  void submit(Configuration driverConf);
}
