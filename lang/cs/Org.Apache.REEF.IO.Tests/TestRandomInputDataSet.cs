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

using System.Collections.Generic;
using System.IO;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.REEF.IO.PartitionedData;
using Org.Apache.REEF.IO.PartitionedData.Random;
using Org.Apache.REEF.Tang.Implementations.Tang;

namespace Org.Apache.REEF.IO.Tests
{
    /// <summary>
    /// Tests for Org.Apache.REEF.IO.PartitionedData.Random.
    /// </summary>
    [TestClass]
    public sealed class TestRandomInputDataSet
    {
        /// <summary>
        /// The number of partitions the tested dataset will have.
        /// </summary>
        private const int NumberOfPartitions = 3;

        /// <summary>
        /// The number of doubles the stream of each partition will return.
        /// </summary>
        private const int NumberOfDoublesPerPartition = 7;

        /// <summary>
        /// Number of bytes per double in the stream.
        /// </summary>
        private const int BytesPerDouble = 8;

        /// <summary>
        /// The number of bytes we expect each partition's stream to return.
        /// </summary>
        private const int ExpectedNumberOfBytesPerPartition = NumberOfDoublesPerPartition * BytesPerDouble;

        /// <summary>
        /// Test for the driver side APIs of RandomDataSet.
        /// </summary>
        [TestMethod]
        public void TestDriverSide()
        {
            var dataSet = MakeRandomDataSet();
            Assert.IsNotNull(dataSet);
            Assert.IsNotNull(dataSet.Id);
            Assert.IsNotNull(dataSet.GetEnumerator());
            Assert.AreEqual(NumberOfPartitions, dataSet.Count);

            IEnumerator<IPartitionDescriptor> desriptors = dataSet.GetEnumerator();
            while (desriptors.MoveNext())
            {
                var descriptor1 = desriptors.Current;
                var descriptor2 = dataSet.GetPartitionDescriptorForId(descriptor1.Id);
                Assert.AreEqual(descriptor1, descriptor2);
            }
        }

        /// <summary>
        /// Tests the Evaluator side of the IPartionedDataSet.
        /// </summary>
        /// <remarks>
        /// This instantiates each IInputPartition using the IConfiguration provided by the IPartitionDescriptor.
        /// </remarks>
        [TestMethod]
        public void TestEvaluatorSide()
        {
            var dataSet = MakeRandomDataSet();
            foreach (var partitionDescriptor in dataSet)
            {
                var partition =
                    TangFactory.GetTang()
                        .NewInjector(partitionDescriptor.GetPartitionConfiguration())
                        .GetInstance<IInputPartition<Stream>>();
                Assert.IsNotNull(partition);
                Assert.IsNotNull(partition.Id);

                using (var partitionStream = partition.GetPartitionHandle())
                {
                    Assert.IsNotNull(partitionStream);
                    Assert.IsTrue(partitionStream.CanRead);
                    Assert.IsFalse(partitionStream.CanWrite);
                    Assert.AreEqual(ExpectedNumberOfBytesPerPartition, partitionStream.Length);
                }
            }
        }

        /// <summary>
        /// Make a DataSet instance using the RandomDataConfiguration.
        /// </summary>
        /// <returns></returns>
        private IPartitionedInputDataSet MakeRandomDataSet()
        {
            return TangFactory.GetTang().NewInjector(RandomInputDataConfiguration.ConfigurationModule
                .Set(RandomInputDataConfiguration.NumberOfDoublesPerPartition, NumberOfDoublesPerPartition.ToString())
                .Set(RandomInputDataConfiguration.NumberOfPartitions, NumberOfPartitions.ToString())
                .Build()).GetInstance<IPartitionedInputDataSet>();
        }
    }
}