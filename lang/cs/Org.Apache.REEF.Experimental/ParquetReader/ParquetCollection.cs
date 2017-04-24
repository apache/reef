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
using System.IO;
using Microsoft.Hadoop.Avro.Container;
using System.Collections;
using System.Collections.Generic;

namespace Org.Apache.REEF.Experimental.ParquetCollection
{
    /// <summary>
    /// Represents a data collection of type T from parquet file.
    /// </summary>
    public class ParquetCollection<T> : IEnumerable<T>, IDisposable
    {
        private readonly Stream stream;
        private readonly IAvroReader<T> avroReader;
        private readonly SequentialReader<T> seqReader;
        private readonly string _avroPath;
        private bool _disposed = false;

        /// <summary>
        /// Constructor of ParquetCollection
        /// </summary>
        /// <param name="avroPath">Path to input avro file.</param>
        public ParquetCollection(string avroPath)
        {
            stream = new FileStream(avroPath, FileMode.Open);
            avroReader = AvroContainer.CreateReader<T>(stream);
            seqReader = new SequentialReader<T>(avroReader);
            _avroPath = avroPath;
        }

        /// <summary>
        /// Method to return an IEnumerator from parquet data.
        /// </summary>
        /// <returns>
        /// Return a IEnumerator that represents parquet data.
        /// </returns>
        public IEnumerator GetEnumerator()
        {
            return seqReader.Objects.GetEnumerator();
        }

        /// <summary>
        /// Method to return an IEnumerator<T> from parquet data.
        /// </summary>
        /// <returns>
        /// Return a IEnumerator<T> that represents parquet data.
        /// </returns>
        IEnumerator<T> IEnumerable<T>.GetEnumerator()
        {
            return seqReader.Objects.GetEnumerator();
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    stream.Close();
                    stream.Dispose();
                    avroReader.Dispose();
                    seqReader.Dispose();
                    File.Delete(_avroPath);
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
