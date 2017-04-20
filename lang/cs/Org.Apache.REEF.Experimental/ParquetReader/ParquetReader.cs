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
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Experimental.ParquetReader.Parameters;
using Org.Apache.REEF.Experimental.ParquetCollection;

namespace Org.Apache.REEF.Experimental.ParquetReader
{
    /// <summary>
    /// Constructs a Parquet file reader.
    /// </summary>
    public class ParquetReader : IDisposable
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(ParquetReader));

        private bool _disposed = false;

        private class JavaProcessConfiguration
        {
            public const string javaExecutable = "java";
            public const string mainClass = "org.apache.reef.experimental.parquet.ParquetReader";
            public string ParquetPath { get; set; }
            public string ClassPath { get; set; }
        }

        private class JavaProcess
        {
            public string AvroPath { get; set; }
            public JavaProcessConfiguration Conf { get; set; }
            public void Start()
            {
                var p = new Process();
                p.StartInfo.FileName = JavaProcessConfiguration.javaExecutable;
                p.StartInfo.Arguments =
                    new[] { "-cp", Conf.ClassPath, JavaProcessConfiguration.mainClass, Conf.ParquetPath, AvroPath }
                    .Aggregate((a, b) => a + ' ' + b);

                Logger.Log(Level.Info, "Running Command: java {0}", p.StartInfo.Arguments);

                p.Start();
                p.WaitForExit();
                p.Dispose();
            }
        }

        private readonly JavaProcessConfiguration c;

        /// <summary>
        /// Constructor of ParquetReader for Tang Injection
        /// </summary>
        /// <param name="parquetPath">Path to input parquet file.</param>
        /// <param name="jarPath">Path to jar file that contains Java parquet reader.</param>
        [Inject]
        private ParquetReader(
            [Parameter(typeof(ParquetPathString))] string parquetPath,
            [Parameter(typeof(ClassPathString))] string classPath)
        {
            if (!File.Exists(parquetPath))
            {
                throw new FileNotFoundException("Input parquet file {0} doesn't exist.", parquetPath);
            }
            c = new JavaProcessConfiguration
            {
                ParquetPath = parquetPath,
                ClassPath = classPath
            };
        }

        /// <summary>
        /// Method to read the given parquet files.
        /// </summary>
        /// <returns>
        /// Return a ParquetCollection that can iterate data from each avro block.
        /// </returns>
        public ParquetCollection<T> Read<T>()
        {
            var p = new JavaProcess
            {
                AvroPath = Path.GetTempFileName(),
                Conf = c
            };

            p.Start();

            return new ParquetCollection<T>(p.AvroPath);
        }

        private void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                }
                _disposed = true;
            }
        }

        /// <summary>
        /// Method to dispose this class.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }
}
