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
package com.microsoft.wake;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;


import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.formats.ConfigurationFile;
import com.microsoft.wake.exception.WakeRuntimeException;

/**
 * Wake parameter configuration
 */
public final class WakeConfiguration {
  private final static Logger LOG = Logger.getLogger(WakeConfiguration.class.getName());
  
  @NamedParameter(doc="Configuration file name", default_value="")
  public final static class FileName implements Name<String> {}
  
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
}
