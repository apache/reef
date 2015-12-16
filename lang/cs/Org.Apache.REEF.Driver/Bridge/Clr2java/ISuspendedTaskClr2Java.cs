/**
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

using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Driver.Bridge.Clr2java
{
    [Private, Interop("SuspendedTaskClr2Java.cpp", "Clr2JavaImpl.h")]
    public interface ISuspendedTaskClr2Java : IClr2Java, IClr2JavaTaskMessage
    {
        /// <summary>
        /// get active context the task is running in
        /// </summary>
        /// <returns>active context</returns>
        IActiveContextClr2Java GetActiveContext();

        /// <summary>
        /// get suspsended task id
        /// </summary>
        /// <returns>suspsended task id</returns>
        string GetId();
    }
}
