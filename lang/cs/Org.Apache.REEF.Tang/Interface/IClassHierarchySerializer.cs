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

using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.ClassHierarchy;

namespace Org.Apache.REEF.Tang.Interface
{
    [DefaultImplementation(typeof(AvroClassHierarchySerializer), "default")]
    public interface IClassHierarchySerializer
    {
        /// <summary>
        /// Serialize a ClassHierarchy into a file 
        /// </summary>
        /// <param name="c"></param>
        /// <param name="fileName"></param>
        void ToFile(IClassHierarchy c, string fileName);

        /// <summary>
        /// Serialize a ClassHierarchy into a text file as Jason string
        /// </summary>
        /// <param name="c"></param>
        /// <param name="fileName"></param>
        void ToTextFile(IClassHierarchy c, string fileName);

        /// <summary>
        /// Serialize a ClassHierarchy into a Jason string
        /// </summary>
        /// <param name="c"></param>
        /// <returns></returns>
        string ToString(IClassHierarchy c);

        /// <summary>
        /// Serialize a ClassHierarchy into a byte array
        /// </summary>
        /// <param name="c"></param>
        /// <returns></returns>
        byte[] ToByteArray(IClassHierarchy c);

        /// <summary>
        /// Deserialize a ClassHierarchy from a file
        /// </summary>
        /// <param name="fileName"></param>
        /// <returns></returns>
        IClassHierarchy FromFile(string fileName);

        /// <summary>
        /// Deserialize a ClassHierarchy from a text file
        /// </summary>
        /// <param name="fileName"></param>
        /// <returns></returns>
        IClassHierarchy FromTextFile(string fileName);

        /// <summary>
        /// Deserialize a ClassHierarchy from a byte array
        /// </summary>
        /// <param name="bytes"></param>
        /// <returns></returns>
        IClassHierarchy FromByteArray(byte[] bytes);

        /// <summary>
        /// Deserialize a ClassHierarchy from a Jason string
        /// </summary>
        /// <param name="jsonString"></param>
        /// <returns></returns>
        IClassHierarchy FromString(string jsonString);
    }
}