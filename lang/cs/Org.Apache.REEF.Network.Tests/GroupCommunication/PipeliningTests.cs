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

using Org.Apache.REEF.Network.Group.Config;
using Org.Apache.REEF.Network.Group.Pipelining;
using Org.Apache.REEF.Network.Group.Pipelining.Impl;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using System;
using System.Collections.Generic;
using Xunit;

namespace Org.Apache.REEF.Network.Tests.GroupCommunication
{
    /// <summary>
    /// Defines pipelining component tests
    /// </summary>
    public class PipeliningTests
    {
        /// <summary>
        /// Test the ArrayPipelineConverter with floats
        /// </summary>
        [Fact]
        public void TestFloatArrayPipelineDataConverter()
        {
            float[] testArray = { 0.1f, 0.2f, 0.3f, 0.4f, 0.5f };
            TestArrayPipelineDataConverter(testArray);
        }

        /// <summary>
        /// Test the ArrayPipelineConverter with doubles
        /// </summary>
        [Fact]
        public void TestDoubleArrayPipelineDataConverter()
        {
            double[] testArray = { 0.1, 0.2, 0.3, 0.4, 0.5 };
            TestArrayPipelineDataConverter(testArray);
        }

        /// <summary>
        /// Test the ArrayPipelineConverter with ints
        /// </summary>
        [Fact]
        public void TestIntArrayPipelineDataConverter()
        {
            int[] testArray = { 1, 2, 3, 4, 5 };
            TestArrayPipelineDataConverter(testArray);
        }

        /// <summary>
        /// Test the ArrayPipelineConverter with longs
        /// </summary>
        [Fact]
        public void TestLongArrayPipelineDataConverter()
        {
            long[] testArray = { 1L, 2L, 3L, 4L, 5L };
            TestArrayPipelineDataConverter(testArray);
        }

        /// <summary>
        /// Test the ArrayPipelineConverter with generic objects
        /// </summary>
        [Fact]
        public void TestObjectArrayPipelineDataConverter()
        {
            object[] testArray = 
            {
                new { A = 1, B = 2, C = 3 },
                new { A = 2, B = 3, C = 4 },
                new { A = 3, B = 4, C = 5 },
                new { A = 4, B = 5, C = 6 },
                new { F = 5, G = 6, H = 7 }
            };
            TestArrayPipelineDataConverter(testArray);
        }

        /// <summary>
        /// Test the ArrayPipelineConverter with an empty array
        /// </summary>
        [Fact]
        public void TestArrayPipelineDataConverterWithEmptyArray()
        {
            object[] testArray = new object[0];
            TestArrayPipelineDataConverter(testArray);
        }

        /// <summary>
        /// Test the ArrayPipelineConverter with a null array
        /// </summary>
        [Fact]
        public void TestArrayPipelineDataConverterWithNullArray()
        {
            object[] testArray = null;
            TestArrayPipelineDataConverter(testArray);
        }

        /// <summary>
        /// Master test function for testing types of arrays in the ArrayPipelineDataConverter
        /// </summary>
        /// <typeparam name="T">The type of array to test</typeparam>
        /// <param name="originalArray">An array to use in the test</param>
        private static void TestArrayPipelineDataConverter<T>(T[] originalArray) where T : new()
        {
            // Verify that the constructor has the proper restrictions
            AssertPositivePipelinePackageElementsRequired<T[], ArrayPipelineDataConverter<T>>();

            // Verify that the FullMessage method properly handles null and zero-length arrays
            AssertNullAndZeroLengthArraysHandledByFullMessage<T, ArrayPipelineDataConverter<T>>();

            // Test the valid case where we break up the array into smaller pieces
            // First determine how many messages to create from originalArray
            int pipelineMessageSize;
            int nMessages;
            if (originalArray == null)
            {
                nMessages = 0;
                pipelineMessageSize = 1;
            }
            else if (originalArray.Length == 0 || originalArray.Length == 1)
            {
                nMessages = 1;
                pipelineMessageSize = 1; // Necessary to instantiate the ArrayPipelineDataConverterConfig
            }
            else
            {
                nMessages = 2;
                pipelineMessageSize = (int)Math.Ceiling(originalArray.Length / (double)nMessages);
            }

            // Test that the valid configuration can be injected
            IConfiguration config = GetPipelineDataConverterConfig(pipelineMessageSize);
            IPipelineDataConverter<T[]> dataConverter = TangFactory.GetTang().NewInjector(config).GetInstance<ArrayPipelineDataConverter<T>>();

            var pipelineData = dataConverter.PipelineMessage(originalArray);

            // Validate that the pipeline constructed the correct number of messages
            Assert.Equal<int>(pipelineData.Count, nMessages);

            T[] deserializedArray = dataConverter.FullMessage(pipelineData);

            // Validate that the array is unaffected by the serialization / deserialization
            AssertArrayEquality(originalArray, deserializedArray);

            // Verify that the "IsLast" property is set correctly
            AssertIsLastFlag(pipelineData);
        }

        /// <summary>
        /// Verify that the IPipelineDataConverter<T> class requires a positive value for pipelinePackageElements
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="DataConverter"></typeparam>
        private static void AssertPositivePipelinePackageElementsRequired<T, DataConverter>()
            where DataConverter : class, IPipelineDataConverter<T>
        {
            // Verify that the PipelinePackageElements cannot be zero
            var configWithZeroElements = GetPipelineDataConverterConfig(0);
            Assert.Throws<InjectionException>(() => TangFactory.GetTang().NewInjector(configWithZeroElements).GetInstance<DataConverter>());

            // Verify that the PipelinePackageElements cannot be less than 0
            var configWithNegativeElements = GetPipelineDataConverterConfig(-2);
            Assert.Throws<InjectionException>(() => TangFactory.GetTang().NewInjector(configWithNegativeElements).GetInstance<DataConverter>());
        }

        /// <summary>
        /// Verify that the FullMessage method properly handles null and zero-length arrays
        /// </summary>
        /// <typeparam name="T">The type of array the IPipelineDataConverter converts</typeparam>
        /// <typeparam name="DataConverter">The IPipelineDataConverter implementation</typeparam>
        private static void AssertNullAndZeroLengthArraysHandledByFullMessage<T, DataConverter>()
            where T : new()
            where DataConverter : class, IPipelineDataConverter<T[]>
        {
            // Test that the valid configuration can be injected
            IConfiguration config = GetPipelineDataConverterConfig(1);
            IPipelineDataConverter<T[]> dataConverter = TangFactory.GetTang().NewInjector(config).GetInstance<ArrayPipelineDataConverter<T>>();

            // Test that a null message returns a null object
            List<PipelineMessage<T[]>> nullMessage = new List<PipelineMessage<T[]>>
            {
                new PipelineMessage<T[]>(null, true)
            };
            Assert.Null(dataConverter.FullMessage(nullMessage));

            // Test that many null messages returns a null object
            List<PipelineMessage<T[]>> manyNullMessages = new List<PipelineMessage<T[]>>
            {
                new PipelineMessage<T[]>(null, false),
                new PipelineMessage<T[]>(null, true)
            };
            Assert.Null(dataConverter.FullMessage(manyNullMessages));

            // Test that null messages mixed with non-null returns the non-null object
            List<PipelineMessage<T[]>> someNullMessages = new List<PipelineMessage<T[]>>
            {
                new PipelineMessage<T[]>(null, false),
                new PipelineMessage<T[]>(new T[2], false),
                new PipelineMessage<T[]>(null, true)
            };
            Assert.Equal(2, dataConverter.FullMessage(someNullMessages).Length);

            // Test that a zero-length message returns a zero-length object
            List<PipelineMessage<T[]>> zeroLengthMessage = new List<PipelineMessage<T[]>>
            {
                new PipelineMessage<T[]>(new T[0], true)
            };
            Assert.Equal(0, dataConverter.FullMessage(zeroLengthMessage).Length);

            // Test that many zero-length message return a zero-length object
            List<PipelineMessage<T[]>> manyZeroLengthMessages = new List<PipelineMessage<T[]>>
            {
                new PipelineMessage<T[]>(new T[0], false),
                new PipelineMessage<T[]>(new T[0], true)
            };
            Assert.Equal(0, dataConverter.FullMessage(manyZeroLengthMessages).Length);
        }

        /// <summary>
        /// Validate that the IsLast flag is properly set on a list of pipeline messages
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="pipelineData">A list of PipelineMessages</param>
        private static void AssertIsLastFlag<T>(IList<PipelineMessage<T>> pipelineData)
        {
            // Verify that the "IsLast" property is set correctly
            for (int i = 0; i < pipelineData.Count; i++)
            {
                Assert.Equal(pipelineData[i].IsLast, i == pipelineData.Count - 1);
            }
        }

        /// <summary>
        /// Generic array equality method; Equality for type T must make sense for this to make sense
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="expected"></param>
        /// <param name="actual"></param>
        private static void AssertArrayEquality<T>(T[] expected, T[] actual)
        {
            // Two null arrays are considered to be equal
            // Check to make sure that the arrays are both defined or undefined
            Assert.True((expected == null) == (actual == null));

            // If the arrays are both null, then don't check any further
            if (expected == null && actual == null)
            {
                return;
            }

            Assert.Equal(expected.Length, actual.Length);

            for (int i = 0; i < actual.Length; i++)
            {
                Assert.True(EqualityComparer<T>.Default.Equals(expected[i], actual[i]));
            }
        }

        /// <summary>
        /// Create a configuration with a PipelinePackageElements parameter
        /// </summary>
        /// <param name="pipelineMessageSize">The length of the individual messages in the pipeline</param>
        /// <returns></returns>
        private static IConfiguration GetPipelineDataConverterConfig(int pipelineMessageSize)
        {
            return TangFactory.GetTang()
                    .NewConfigurationBuilder()
                    .BindNamedParameter(typeof(GroupCommConfigurationOptions.PipelineMessageSize), pipelineMessageSize.ToString())
                    .Build();
        }
    }
}
