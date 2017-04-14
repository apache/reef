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
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using System.IO;
using System.Collections.Generic;
using Microsoft.Hadoop.Avro.Container;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Experimental.ParquetReader
{
    sealed public class ParquetReader
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(ParquetReader));

        private readonly string _parquetPath;
        private readonly string _avroPath;
        private readonly string _jarPath;

        private class JavaProcessFactory
        {
            public string fileName { get; set; }
            public string parquetPath { get; set; }
            public string avroPath { get; set; }
            public string jarPath { get; set; }
            public string mainClass { get; set; }
            public Process p { get; set; }
        }

        /// <summary>
        /// Constructor of ParquetReader for Tang Injection
        /// </summary>
        /// <param name="parquetPath">Path to input parquet file.</param>
        /// <param name="avroPath">Path to temp avro file.</param>
        /// <param name="jarPath">Path to jar file that contains Java parquet reader.</param>
        [Inject]
        private ParquetReader(
            [Parameter(typeof(ParquetPathString))] string parquetPath, 
            [Parameter(typeof(AvroPathString))] string avroPath,
            [Parameter(typeof(JarPathString))] string jarPath)
        {
            _parquetPath = parquetPath;
            _avroPath = avroPath;
            _jarPath = jarPath;
        }

        /// <summary>
        /// Method to read the given parquet files.
        /// </summary>
        public IEnumerable<T> read<T>()
        {
            JavaProcessFactory f = new JavaProcessFactory
            {
                fileName = "java",
                parquetPath = _parquetPath,
                avroPath = _avroPath,
                jarPath = _jarPath,
                mainClass = "org.apache.reef.experimental.parquet.ParquetReader",
                p = new Process()
            };

            f.p.StartInfo.FileName = f.fileName;
            f.p.StartInfo.Arguments = $"-cp {f.jarPath} {f.mainClass} {f.parquetPath} {f.avroPath}";
            f.p.Start();
            f.p.WaitForExit();

            Stream stream = new FileStream(_avroPath, FileMode.Open);
            using (var reader = AvroContainer.CreateReader<T>(stream))
            {
                using (var streamReader = new SequentialReader<T>(reader))
                {
                    return streamReader.Objects;
                }
            }
        }
    }
}
