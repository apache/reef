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
 * Provides the interfaces for MPI style group communication operations.
 * The interface is asymmetric for asymmetric operations and symmetric
 * for symmetric operations unlike MPI which provides symmetric operations
 * for all operations.
 *
 * The interface is asymmetric in the sense that Senders & Receivers are
 * separated out for operations like Scatter and Gather. All participants
 * do not execute the same function. A sender sends & a receiver receives.
 *
 * The interface only concentrates on the data part because we are on the
 * data-plane of things in REEF. The control information is embedded in the
 * {@link com.microsoft.tang.Configuration} used to instantiate these
 * operators. It is the responsibility of the Driver, the primary agent in
 * the control-plane to configure these operators, that is, denote who is
 * the sender, who are the receivers, what {@link com.microsoft.reef.io.serialization.Codec}
 * need to be used and so on for an operation like Scatter with the root node
 * acting as a sender and the other nodes as receivers.
 *
 * One thing implicit in MPI operations is the ordering of processors based
 * on their ranks which determines the order of operations. For ex., if we
 * scatter an array of 10 elements into 10 processors, then which processor
 * gets the 1st entry & so on is based on the rank.
 *
 * In our case we do not have any ranks associated with tasks. Instead,
 * by default we use the lexicographic order of the task ids. These can
 * also be over-ridden in the send/receive/apply function calls
 */
package com.microsoft.reef.io.network.group.operators;
