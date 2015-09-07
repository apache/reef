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
package org.apache.reef.io.watcher;

import org.apache.reef.annotations.Unstable;
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * Write events to a certain destination.
 */
@Unstable
@DefaultImplementation(LogEventStream.class)
public interface EventStream {

  /**
   * Write an eventRecord with specific type. This should be thread-safe
   * since multiple event handlers can concurrently call the method.
   *
   * @param type a event type
   * @param jsonEncodedEvent an event encoded as json
   */
  void onEvent(EventType type, String jsonEncodedEvent);

}
