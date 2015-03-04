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

using Org.Apache.REEF.Tang.Annotations;
using System;

namespace Org.Apache.REEF.Network.Group.CommonOperators.Driver.PipelineDataConverters
{
    public class PipelineOptions
    {
        public static readonly int ChunkSizeInteger = 16384;
        public static readonly int ChunkSizeFloat = 16384;
        public static readonly int ChunkSizeDouble = 8192;
        
        [NamedParameter("Chunk size for integer array", "intchunksize", "16384")]
        public class IntegerChunkSize : Name<int>
        {
        }

        [NamedParameter("Chunk size for float array", "floatchunksize", "16384")]
        public class FloatChunkSize : Name<int>
        {
        }

        [NamedParameter("Chunk size for double array", "doublechunksize", "8192")]
        public class DoubleChunkSize : Name<int>
        {
        }
    }
}
