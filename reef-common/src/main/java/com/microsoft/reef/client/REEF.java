/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.client;

import com.microsoft.reef.annotations.audience.ClientSide;
import com.microsoft.reef.annotations.audience.Public;
import com.microsoft.reef.runtime.common.client.REEFImplementation;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.annotations.DefaultImplementation;

/**
 * The main entry point into the REEF resourcemanager.
 * <p/>
 * Every REEF resourcemanager provides an implementation of this interface. That
 * instance is used to submitTask the Driver class for execution to REEF. As with
 * all submissions in REEF, this is done in the form of a TANG Configuration
 * object.
 */
@Public
@ClientSide
@DefaultImplementation(REEFImplementation.class)
public interface REEF extends AutoCloseable {

  static final String REEF_VERSION = "0.6-SNAPSHOT";

  /**
   * Close the resourcemanager connection.
   */
  @Override
  public void close();

  /**
   * Submits the Driver set up in the given Configuration for execution.
   * <p/>
   * The Configuration needs to bind the Driver interface to an actual
   * implementation of that interface for the job at hand.
   *
   * @param driverConf The driver configuration: including everything it needs to execute.  @see DriverConfiguration
   */
  public void submit(final Configuration driverConf);

  /**
   * @return the version of REEF running.
   */
  public String getVersion();
}
