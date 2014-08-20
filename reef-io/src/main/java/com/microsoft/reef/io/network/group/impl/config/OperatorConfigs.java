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
package com.microsoft.reef.io.network.group.impl.config;

import com.microsoft.tang.Configuration;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.wake.ComparableIdentifier;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Map from Id to List of {@link Configuration}
 * with some extensions like check and put
 * and additional methods to add {@link Configuration}s
 * to {@link JavaConfigurationBuilder}
 */
public class OperatorConfigs extends
    HashMap<ComparableIdentifier, List<Configuration>> {

  /**
   * serialization version
   */
  private static final long serialVersionUID = 556190775377740767L;

  /**
   * Check and put - If the id is not contained, create a new list and add the conf to it else add it to the existing one
   *
   * @param id
   * @param conf
   */
  public void put(ComparableIdentifier id, Configuration conf) {
    List<Configuration> confs = !containsKey(id) ? new ArrayList<Configuration>() : get(id);
    confs.add(conf);
    super.put(id, confs);
  }

  /**
   * Add configurations corresponding to id into the {@link JavaConfigurationBuilder}
   *
   * @param id
   * @param jcb
   * @throws BindException
   */
  public void addConfigurations(ComparableIdentifier id, JavaConfigurationBuilder jcb) throws BindException {
    for (Configuration conf : get(id))
      jcb.addConfiguration(conf);
  }
}
