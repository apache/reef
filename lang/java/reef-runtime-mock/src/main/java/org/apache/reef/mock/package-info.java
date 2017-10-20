/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */
/**
 * Mock runtime API.
 *
 * Mock runtime is meant to mimic the semantics of the REEF runtime and
 * allow:
 *  1. Applications to driver the forward progress of processing REEF events.
 *     See {@link org.apache.reef.mock.MockRuntime} API
 *  2. Control the advancement of the Clock and Alarm callbacks.
 *     See {@link org.apache.reef.mock.MockClock}
 *  3. Inject failures into the REEF applications.
 *     See {@link org.apache.reef.mock.MockFailure}
 *
 * Use {@link org.apache.reef.mock.MockConfiguration} to bind your REEF
 * driver application event handlers.
 *
 * Use {@link org.apache.reef.mock.MockRuntime#start()} to trigger the
 * driver start event and {@link org.apache.reef.mock.MockRuntime#stop()}}
 * or {@link org.apache.reef.mock.MockClock#close()} to trigger the driver
 * stop event.
 */
package org.apache.reef.mock;
