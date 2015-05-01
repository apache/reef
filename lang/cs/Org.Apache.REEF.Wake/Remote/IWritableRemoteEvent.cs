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

using System;
using System.Linq.Expressions;
using System.Net;

namespace Org.Apache.REEF.Wake.Remote
{
    /// <summary>
    /// Interface for remote event
    /// </summary>
    /// <typeparam name="T">Type of remote event message. It is assumed that T implements IWritable</typeparam>
    [Obsolete("Need to remove Iwritable and use IstreamingCodec. Please see Jira REEF-295 ", false)]
    public interface IWritableRemoteEvent<T> : IWritable where T : IWritable
    {
        /// <summary>
        /// Local Endpoint
        /// </summary>
        IPEndPoint LocalEndPoint { get; set; }

        /// <summary>
        /// Remote Endpoint
        /// </summary>
        IPEndPoint RemoteEndPoint { get; set; }

        string Source { get; }

        string Sink { get; }

        T Value { get; }

        long Sequence { get; }
    }
}
