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

/**
 * Elastic Group Communications for REEF.
 *
 * Provides MPI style Group Communication operators for collective communication
 * between tasks. These should be primarily used for any form of
 * task to task messaging along with the point to point communication
 * provided by {@link org.apache.reef.io.network.impl.NetworkService}
 *
 * The interfaces for the operators are in org.apache.reef.io.network.group.api.operators
 * The fluent way to describe these operators is available org.apache.reef.io.network.group.config
 * The implementation of these operators are available in org.apache.reef.io.network.group.impl
 * Currently only a basic implementation is available
 */
package org.apache.reef.io.network.group;
