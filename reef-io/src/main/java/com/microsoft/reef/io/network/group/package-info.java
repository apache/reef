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
/**
 * Provides MPI style Group Communication operators for collective communication
 * between tasks. These should be primarily used for any form of
 * task to task messaging along with the point to point communication
 * provided by {@link com.microsoft.reef.io.network.impl.NetworkService}
 *
 * The interfaces for the operators are in com.microsoft.reef.io.network.group.operators
 * The fluent way to describe these operators is available com.microsoft.reef.io.network.group.config
 * The implementation of these operators are available in com.microsoft.reef.io.network.group.impl
 * Currently only a basic implementation is available
 */
package com.microsoft.reef.io.network.group;
