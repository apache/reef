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
using System.Diagnostics;
using System.IO;
using System.Collections.Generic;
using Microsoft.Hadoop.Avro.Container;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Experimental.ParquetReader
{
    sealed public class ParquetReader : IDisposable
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(ParquetReader));

        private bool _disposed = false;

        private FileStream stream = null;

        private class JavaProcessFactory
        {
            public string fileName { get; set; }
            public string mainClass { get; set; }
            public string parquetPath { get; set; }
            public string jarPath { get; set; }
        }

        private JavaProcessFactory f;

        /// <summary>
        /// Constructor of ParquetReader for Tang Injection
        /// </summary>
        /// <param name="parquetPath">Path to input parquet file.</param>
        /// <param name="jarPath">Path to jar file that contains Java parquet reader.</param>
        [Inject]
        private ParquetReader(
            [Parameter(typeof(ParquetPathString))] string parquetPath,
            [Parameter(typeof(JarPathString))] string jarPath)
        {
            f = new JavaProcessFactory
            {
                fileName = "java",
                mainClass = "org.apache.reef.experimental.parquet.ParquetReader",
                parquetPath = parquetPath,
                jarPath = jarPath
            };
        }

        /// <summary>
        /// Method to read the given parquet files.
        /// </summary>
        public IEnumerable<T> Read<T>()
        {
            var avroPath = Path.GetTempFileName();

            Process proc = new Process();
            proc.StartInfo.FileName = f.fileName.ToString();
            proc.StartInfo.Arguments = 
                new[] { "-cp", f.jarPath, f.mainClass, f.parquetPath, avroPath }.Aggregate((a, b) => a + ' ' + b);

            Logger.Log(Level.Info, "Running Command: java {0}", proc.StartInfo.Arguments);

            proc.Start();
            proc.WaitForExit();
            proc.Dispose();

            stream = new FileStream(avroPath, FileMode.Open);
            using (var avroReader = AvroContainer.CreateReader<T>(stream))
            {
                using (var seqReader = new SequentialReader<T>(avroReader))
                {
                    return seqReader.Objects;
                }
            }
        }

        private void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    stream.Dispose();
                    stream = null;
                }
                f = null;
                _disposed = true;
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }
}
