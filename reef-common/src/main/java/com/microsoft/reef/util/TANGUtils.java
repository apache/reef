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
package com.microsoft.reef.util;

import com.microsoft.tang.Configuration;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.formats.ConfigurationFile;

public final class TANGUtils {

  private TANGUtils() {
  }

  public static Configuration fromString(final String s) {
    try {
      final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
      ConfigurationFile.addConfiguration(cb, s);
      return cb.build();
    } catch (BindException e) {
      throw new RuntimeException(e);
    }
  }

  public static Configuration merge(final Configuration... confs) {
    try {
      return Tang.Factory.getTang().newConfigurationBuilder(confs).build();
    } catch (BindException e) {
      throw new RuntimeException(e);
    }
  }

  public static Configuration fromStringEncoded(final String s) {
    return fromString(s.replaceAll("#", "\n"));
  }

  public static String toStringEncoded(final Configuration c) {
    final String cstr = ConfigurationFile.toConfigurationString(c);
    try {
      return cstr.replaceAll("\n", "#");
    } catch (Throwable t) {
      System.out.println("CONFIGURATION STRING: " + cstr);
      throw t;
    }
  }
}
