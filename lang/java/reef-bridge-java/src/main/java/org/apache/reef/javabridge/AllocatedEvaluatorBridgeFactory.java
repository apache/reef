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
package org.apache.reef.javabridge;

import net.jcip.annotations.GuardedBy;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.tang.ClassHierarchy;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;

import javax.inject.Inject;

/**
 * The Factory object used to create AllocatedEvaluatorBridge objects.
 * AllocatedEvaluatorBridge objects should not be instantiated directly.
 */
@DriverSide
@Private
public final class AllocatedEvaluatorBridgeFactory {
  @GuardedBy("this")
  private ClassHierarchy clrClassHierarchy;
  private AvroConfigurationSerializer serializer;

  /**
   * This is always instantiated via Tang.
   */
  @Inject
  private AllocatedEvaluatorBridgeFactory(final AvroConfigurationSerializer serializer) {
    this.serializer = serializer;
  }


  /**
   * Instantiates a new AllocatedEvaluatorBridge.
   * @param allocatedEvaluator the AllocatedEvaluator object.
   * @param serverInfo the name server String.
   * @return the allocatedEvaluatorBridge.
   */
  public AllocatedEvaluatorBridge getAllocatedEvaluatorBridge(final AllocatedEvaluator allocatedEvaluator,
                                                              final String serverInfo) {
    return new AllocatedEvaluatorBridge(allocatedEvaluator, getClrClassHierarchy(), serverInfo, serializer);
  }

  /**
   * Returns the clr ClassHierarchy. Loads it if needed.
   *
   * @return the clr ClassHierarchy.
   */
  private synchronized ClassHierarchy getClrClassHierarchy() {
    if (null == this.clrClassHierarchy) {
      this.clrClassHierarchy = Utilities.loadClassHierarchy(NativeInterop.CLASS_HIERARCHY_FILENAME);
    }
    return this.clrClassHierarchy;
  }
}
