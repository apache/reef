/**
 * Copyright (C) 2013 Microsoft Corporation
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
package com.microsoft.reef.driver;

import com.microsoft.tang.Configuration;
import com.microsoft.tang.ConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.formats.ConfigurationFile;

import java.io.File;

public class Utils {

  public static Configuration roundtrip(final Configuration conf) {
    try {
      final File f = File.createTempFile("TANGConf-", ".conf");
      ConfigurationFile.writeConfigurationFile(conf, f);
      final ConfigurationBuilder b = Tang.Factory.getTang().newConfigurationBuilder();
      ConfigurationFile.addConfiguration(b, f);
      return b.build();
    } catch (final Exception e) {
      throw new RuntimeException("Unable to roundtrip a TANG Configuration.", e);
    }
  }

}
