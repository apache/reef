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
using System.Threading;
using System.Threading.Tasks;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.StreamingCodec;

namespace Org.Apache.REEF.Wake.Remote.Impl
{
    // TODO: This class will be removed shortly and is used to reduce the size of pull request.
    internal sealed class TemporaryWritableToStreamingCodec<T> : IStreamingCodec<T> where T:IWritable
    {
        private readonly IInjector _injector;

        [Inject]
        public TemporaryWritableToStreamingCodec(IInjector injector)
        {
            _injector = injector;
        }

        public T Read(IDataReader reader)
        {
            string type = reader.ReadString();
            var value = (T) _injector.ForkInjector().GetInstance(type);
            value.Read(reader);
            return value;
        }

        public void Write(T obj, IDataWriter writer)
        {
            writer.WriteString(obj.GetType().AssemblyQualifiedName);
            obj.Write(writer);
        }

        public async Task<T> ReadAsync(IDataReader reader, CancellationToken token)
        {
            string type = await reader.ReadStringAsync(token);
            var value = (T) _injector.ForkInjector().GetInstance(type);
            await value.ReadAsync(reader, token);
            return value;
        }

        public async Task WriteAsync(T obj, IDataWriter writer, CancellationToken token)
        {
            await writer.WriteStringAsync(obj.GetType().AssemblyQualifiedName, token);
            await obj.WriteAsync(writer, token);
        }
    }
}
