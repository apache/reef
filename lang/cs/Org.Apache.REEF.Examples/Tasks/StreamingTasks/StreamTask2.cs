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

using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Examples.Tasks.StreamingTasks
{
    public class StreamTask2 : ITask
    {
        [Inject]
        public StreamTask2()
        {
        }

        public byte[] Call(byte[] memento)
        {
            System.Console.WriteLine("Hello, Streaming 2!!");

            SISecondNode();

            return null;
        }

        public void Dispose()
        {
        }

        public void SISecondNode()
        {
            // var a = new SISecondNodeXAM();
            // a.Process(2222, 1111);
        }
    }
}
