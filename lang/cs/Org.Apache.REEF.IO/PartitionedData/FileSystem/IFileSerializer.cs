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

namespace Org.Apache.REEF.IO.PartitionedData.FileSystem
{
    /// <summary>
    /// A interface for user to implement its serializer.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface IFileSerializer<T>
    {
        /// <summary>
        /// The input is a file where the output is to be written
        /// The output handler is of type T which is defined by the client
        /// and knows how to write the user result to the file.
        /// If there is any IO error, IOException could be thrown.
        /// </summary>
        /// <param name="outputFile">local file in which to write the output</param>
        /// <returns>outputHandler that knows how to write output to the file</returns>
        T Serializer(string outputFile);
    }
}
