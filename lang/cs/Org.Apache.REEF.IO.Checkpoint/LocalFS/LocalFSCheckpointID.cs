// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
// 
//   http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Org.Apache.REEF.IO.Checkpoint.LocalFS
{
    class LocalFSCheckpointID : ICheckpointID
    {
        private readonly string _path;

        public LocalFSCheckpointID(string path)
        {
            _path = path;
        }

        public string GetPath()
        {
            return _path;
        }

        public override string ToString()
        {
            return _path;
        }

        public override bool Equals(object other)
        {
            return (other is LocalFSCheckpointID)
            && _path.Equals(((LocalFSCheckpointID)other)._path);
        } 

        public override int GetHashCode()
        {
            return _path.GetHashCode();
        }
    }
}
