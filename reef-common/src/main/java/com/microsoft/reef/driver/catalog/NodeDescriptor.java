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
package com.microsoft.reef.driver.catalog;

import com.microsoft.reef.annotations.Unstable;
import com.microsoft.reef.driver.capabilities.CPU;
import com.microsoft.reef.driver.capabilities.RAM;
import com.microsoft.reef.io.naming.Identifiable;

import java.net.InetSocketAddress;

/**
 * Descriptor of the physical setup of an Evaluator.
 */
@Unstable
public interface NodeDescriptor extends ResourceCatalog.Descriptor, Identifiable {
  /**
   * Access the inet address of the Evaluator.
   *
   * @return the inet address of the Evaluator.
   */
  public InetSocketAddress getInetSocketAddress();

  /**
   * Access the CPU information of this Evaluator.
   *
   * @deprecated in REEF 0.2. As none of the resource managers REEF runs on supports anything beyond memory and CPUs,
   * we will remove this API.
   */
  @Deprecated
  public CPU getCPUCapability();

  /**
   * Access the memory configuration of this Evaluator.
   *
   * @deprecated in REEF 0.2. As none of the resource managers REEF runs on supports anything beyond memory and CPUs,
   * we will remove this API.
   */
  @Deprecated
  public RAM getRAMCapability();

  /**
   * @return the rack descriptor that contains this node
   */
  public RackDescriptor getRackDescriptor();
}
