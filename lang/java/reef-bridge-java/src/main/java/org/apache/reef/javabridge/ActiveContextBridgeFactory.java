/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.reef.javabridge;

import net.jcip.annotations.ThreadSafe;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ClosedContext;
import org.apache.reef.tang.ClassHierarchy;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Factory for ActiveContextBridge instances.
 * <p/>
 * This class is thread safe because all its methods are synchronized. It should only be used within the bridge code.
 */
@DriverSide
@ThreadSafe
@Private
public final class ActiveContextBridgeFactory {
  private static final Logger LOG = Logger.getLogger(ActiveContextBridge.class.getName());

  private final Map<String, ActiveContextBridge> activeContextBridgeCache = new HashMap<>();
  private final ClassHierarchy clrClassHierarchy = Utilities.loadClassHierarchy(NativeInterop.CLASS_HIERARCHY_FILENAME);
  private final AvroConfigurationSerializer configurationSerializer;

  /**
   * This is always instantiated via Tang.
   *
   * @param configurationSerializer passed to the ActiveContextBridge instances for configuration serialization.
   */
  @Inject
  private ActiveContextBridgeFactory(final AvroConfigurationSerializer configurationSerializer) {
    this.configurationSerializer = configurationSerializer;
  }

  /**
   * Instantiates a new ActiveContextBridge or returns the one created earlier.
   *
   * @param context the context for which to return an ActiveContextBridge instance.
   * @return a new ActiveContextBridge or returns the one created earlier.
   */
  public synchronized ActiveContextBridge getActiveContextBridge(final ActiveContext context) {
    final String contextId = context.getId();

    ActiveContextBridge result = activeContextBridgeCache.get(contextId);
    if (null == result) {
      result = new ActiveContextBridge(context, this.clrClassHierarchy, this.configurationSerializer);
      activeContextBridgeCache.put(contextId, result);
    }
    return result;
  }

  /**
   * Removes the ActiveContextBridge associated with the given Context from the cache.
   *
   * @param context the context whose ActiveContextBridge instance shall be removed from the cache.
   */
  public synchronized void ForgetContext(final ClosedContext context) {
    final String contextId = context.getEvaluatorId();
    final ActiveContextBridge removed = this.activeContextBridgeCache.remove(contextId);
    if (null != removed) {
      LOG.warning("Called with context previously unknown: " + contextId);
    }
  }
}
