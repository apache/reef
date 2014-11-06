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
package org.apache.reef.wake;

import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.formats.ConfigurationFile;
import org.apache.reef.wake.exception.WakeRuntimeException;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Wake parameter configuration
 */
public final class WakeConfiguration {
  private final static Logger LOG = Logger.getLogger(WakeConfiguration.class.getName());

  @Inject
  public WakeConfiguration(final @Parameter(FileName.class) String confFileName) {
    if (confFileName.equals("")) {
      LOG.log(Level.WARNING, "The Wake configuration file is not specified.");
    } else {
      Tang t = Tang.Factory.getTang();
      JavaConfigurationBuilder cb = t.newConfigurationBuilder();
      try {
        ConfigurationFile.addConfiguration(cb, new File(confFileName));
      } catch (BindException e) {
        throw new WakeRuntimeException(e);
      } catch (IOException e) {
        throw new WakeRuntimeException(e);
      }
    }
  }

  @NamedParameter(doc = "Configuration file name", default_value = "")
  public final static class FileName implements Name<String> {
  }
}
