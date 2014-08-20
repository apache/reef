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
 * Contains a basic implementation of the interfaces in
 * com.microsoft.reef.io.network.group.operators
 *
 * Also has classes that help in creating {@link com.microsoft.tang.Configuration}
 * for the same implementations in the config package
 *
 * The implementation here are basic in the sense that
 * they treat the tasks to form a single level tree
 * containing the sender or receiver at the root and all
 * other receivers or senders respectively to form leaves
 * of the tree.
 *
 * The Symmetric Operators are implemented as combination
 * of asymmetric operators:
 * AllGather := Gather + Broadcast<List>
 * AllReduce := Reduce + Broadcast<List>
 * ReduceScatter := Reduce + Scatter
 *
 * The state is managed through a hierarchy of objects:
 * SenderReceiverBase (extended by all symmetric operators)
 * |
 * |--SenderBase (extended by senders of asymmetric operators)
 * |
 * |--ReceiverBase (extended by receivers of asymmetric operators)
 */
@java.lang.Deprecated
package com.microsoft.reef.io.network.group.impl.operators.basic;
