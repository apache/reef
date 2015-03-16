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

using System;
using Org.Apache.REEF.Network.Group.Driver;
using Org.Apache.REEF.Network.Group.Pipelining;
using Org.Apache.REEF.Network.Group.Operators.Impl;
using Org.Apache.REEF.Network.Group.Topology;
using Org.Apache.REEF.Network.Group.CommonOperators.Driver.Codecs;
using Org.Apache.REEF.Network.Group.CommonOperators.Driver.PipelineDataConverters;
using Org.Apache.REEF.Network.Group.CommonOperators.Driver.ReduceFunctions;

namespace Org.Apache.REEF.Network.Group.CommonOperators.Driver
{
    /// <summary>
    /// Used to add common reduce operators on int, float, double and their
    /// arrays, and common reduce functions like add,min,max,merge by the driver
    /// </summary>
    public static class AddCommonReduceOperators
    {
        /// <summary>
        /// Adds integer reduce operator with sum as the function
        /// </summary>
        /// <param name="driver"> Communication group driver that this operator 
        /// should be part of</param>
        /// <param name="masterTaskId">Identifer for MPI master task</param>
        /// <param name="operatorName">Optional parameter to give cutsomized name 
        /// to the operator</param>
        public static void AddIntegerSumReduceOperator(ICommunicationGroupDriver driver, 
            string masterTaskId,
            string operatorName = DefaultCommonOperatorNames.IntSumReduceName)
        {
            driver.AddReduce(
                operatorName,
                new ReduceOperatorSpec<int>(
                    masterTaskId,
                    new IntegerCodec(),
                    new IntegerSumFunction()),
                    TopologyTypes.Flat);
        }

        /// <summary>
        /// Adds integer reduce operator with min as the function
        /// </summary>
        /// <param name="driver"> Communication group driver that this operator 
        /// should be part of</param>
        /// <param name="masterTaskId">Identifer for MPI master task</param>
        /// <param name="operatorName">Optional parameter to give cutsomized name 
        /// to the operator</param>
        public static void AddIntegerMinReduceOperator(ICommunicationGroupDriver driver, 
            string masterTaskId,
            string operatorName = DefaultCommonOperatorNames.IntMinReduceName)
        {
            driver.AddReduce(
                operatorName,
                new ReduceOperatorSpec<int>(
                    masterTaskId,
                    new IntegerCodec(),
                    new IntegerMinFunction()),
                    TopologyTypes.Flat);
        }

        /// <summary>
        /// Adds integer reduce operator with max as the function
        /// </summary>
        /// <param name="driver"> Communication group driver that this operator 
        /// should be part of</param>
        /// <param name="masterTaskId">Identifer for MPI master task</param>
        /// <param name="operatorName">Optional parameter to give cutsomized name 
        /// to the operator</param>
        public static void AddIntegerMaxReduceOperator(ICommunicationGroupDriver driver, 
            string masterTaskId,
            string operatorName = DefaultCommonOperatorNames.IntMaxReduceName)
        {
            driver.AddReduce(
                operatorName,
                new ReduceOperatorSpec<int>(
                    masterTaskId,
                    new IntegerCodec(),
                    new IntegerMaxFunction()),
                    TopologyTypes.Flat);
        }

        /// <summary>
        /// Adds float reduce operator with sum as the function
        /// </summary>
        /// <param name="driver"> Communication group driver that this operator 
        /// should be part of</param>
        /// <param name="masterTaskId">Identifer for MPI master task</param>
        /// <param name="operatorName">Optional parameter to give cutsomized name 
        /// to the operator</param>
        public static void AddFloatSumReduceOperator(ICommunicationGroupDriver driver, 
            string masterTaskId,
            string operatorName = DefaultCommonOperatorNames.FloatSumReduceName)
        {
            driver.AddReduce(
                operatorName,
                new ReduceOperatorSpec<float>(
                    masterTaskId,
                    new FloatCodec(),
                    new FloatSumFunction()),
                    TopologyTypes.Flat);
        }

        /// <summary>
        /// Adds float reduce operator with min as the function
        /// </summary>
        /// <param name="driver"> Communication group driver that this operator 
        /// should be part of</param>
        /// <param name="masterTaskId">Identifer for MPI master task</param>
        /// <param name="operatorName">Optional parameter to give cutsomized name 
        /// to the operator</param>
        public static void AddFloatMinReduceOperator(ICommunicationGroupDriver driver, 
            string masterTaskId,
            string operatorName = DefaultCommonOperatorNames.FloatMinReduceName)
        {
            driver.AddReduce(
                operatorName,
                new ReduceOperatorSpec<float>(
                    masterTaskId,
                    new FloatCodec(),
                    new FloatMinFunction()),
                    TopologyTypes.Flat);
        }

        /// <summary>
        /// Adds float reduce operator with max as the function
        /// </summary>
        /// <param name="driver"> Communication group driver that this operator 
        /// should be part of</param>
        /// <param name="masterTaskId">Identifer for MPI master task</param>
        /// <param name="operatorName">Optional parameter to give cutsomized name 
        /// to the operator</param>
        public static void AddFloatMaxReduceOperator(ICommunicationGroupDriver driver,
            string masterTaskId,
            string operatorName = DefaultCommonOperatorNames.FloatMaxReduceName)
        {
            driver.AddReduce(
                operatorName,
                new ReduceOperatorSpec<float>(
                    masterTaskId,
                    new FloatCodec(),
                    new FloatMaxFunction()),
                    TopologyTypes.Flat);
        }

        /// <summary>
        /// Adds double reduce operator with sum as the function
        /// </summary>
        /// <param name="driver"> Communication group driver that this operator 
        /// should be part of</param>
        /// <param name="masterTaskId">Identifer for MPI master task</param>
        /// <param name="operatorName">Optional parameter to give cutsomized name 
        /// to the operator</param>
        public static void AddDoubleSumReduceOperator(ICommunicationGroupDriver driver, 
           string masterTaskId,
           string operatorName = DefaultCommonOperatorNames.DoubleSumReduceName)
        {
            driver.AddReduce(
                operatorName,
                new ReduceOperatorSpec<double>(
                    masterTaskId,
                    new DoubleCodec(),
                    new DoubleSumFunction()),
                    TopologyTypes.Flat);
        }

        /// <summary>
        /// Adds double reduce operator with min as the function
        /// </summary>
        /// <param name="driver"> Communication group driver that this operator 
        /// should be part of</param>
        /// <param name="masterTaskId">Identifer for MPI master task</param>
        /// <param name="operatorName">Optional parameter to give cutsomized name 
        /// to the operator</param>
        public static void AddDoubleMinReduceOperator(ICommunicationGroupDriver driver, 
            string masterTaskId,
            string operatorName = DefaultCommonOperatorNames.DoubleMinReduceName)
        {
            driver.AddReduce(
                operatorName,
                new ReduceOperatorSpec<double>(
                    masterTaskId,
                    new DoubleCodec(),
                    new DoubleMinFunction()),
                    TopologyTypes.Flat);
        }

        /// <summary>
        /// Adds double reduce operator with max as the function
        /// </summary>
        /// <param name="driver"> Communication group driver that this operator 
        /// should be part of</param>
        /// <param name="masterTaskId">Identifer for MPI master task</param>
        /// <param name="operatorName">Optional parameter to give cutsomized name 
        /// to the operator</param>
        public static void AddDoubleMaxReduceOperator(ICommunicationGroupDriver driver, 
            string masterTaskId,
            string operatorName = DefaultCommonOperatorNames.DoubleMaxReduceName)
        {
            driver.AddReduce(
                operatorName,
                new ReduceOperatorSpec<double>(
                    masterTaskId,
                    new DoubleCodec(),
                    new DoubleMaxFunction()),
                    TopologyTypes.Flat);
        }


        /// <summary>
        /// Adds integer array reduce operator with sum as the function
        /// </summary>
        /// <param name="driver"> Communication group driver that this operator 
        /// should be part of</param>
        /// <param name="masterTaskId">Identifer for MPI master task</param>
        /// <param name="operatorName">Optional parameter to give cutsomized name 
        /// to the operator</param>
        public static void AddIntegerArraySumReduceOperator(ICommunicationGroupDriver driver,
            string masterTaskId,
            string operatorName = DefaultCommonOperatorNames.IntArraySumReduceName)
        {
            driver.AddReduce(
                operatorName,
                new ReduceOperatorSpec<int[]>(
                    masterTaskId,
                    new IntegerArrayCodec(),
                    new PipelineIntDataConverter(PipelineOptions.ChunkSizeInteger),
                    new IntegerArraySumFunction()),
                    TopologyTypes.Flat);
        }

        /// <summary>
        /// Adds integer array reduce operator with merge as the function
        /// The reduce operator in this case will append arrays from each node
        /// in to one big array
        /// </summary>
        /// <param name="driver"> Communication group driver that this operator 
        /// should be part of</param>
        /// <param name="masterTaskId">Identifer for MPI master task</param>
        /// <param name="operatorName">Optional parameter to give cutsomized name 
        /// to the operator</param>
        public static void AddIntegerArrayMergeReduceOperator(ICommunicationGroupDriver driver,
            string masterTaskId,
            string operatorName = DefaultCommonOperatorNames.IntArrayMergeReduceName)
        {
            driver.AddReduce(
                operatorName,
                new ReduceOperatorSpec<int[]>(
                    masterTaskId,
                    new IntegerArrayCodec(),
                    new IntegerArrayMergeFunction()),
                    TopologyTypes.Flat);
        }

        /// <summary>
        /// Adds float array reduce operator with sum as the function
        /// </summary>
        /// <param name="driver"> Communication group driver that this operator 
        /// should be part of</param>
        /// <param name="masterTaskId">Identifer for MPI master task</param>
        /// <param name="operatorName">Optional parameter to give cutsomized name 
        /// to the operator</param>
        public static void AddFloatArraySumReduceOperator(ICommunicationGroupDriver driver, 
            string masterTaskId,
            string operatorName = DefaultCommonOperatorNames.FloatArraySumReduceName)
        {
            driver.AddReduce(
                operatorName,
                new ReduceOperatorSpec<float[]>(
                    masterTaskId,
                    new FloatArrayCodec(),
                    new PipelineFloatDataConverter(PipelineOptions.ChunkSizeFloat),
                    new FloatArraySumFunction()),
                    TopologyTypes.Flat);
        }

        /// <summary>
        /// Adds float array reduce operator with merge as the function
        /// The reduce operator in this case will append arrays from each node
        /// in to one big array
        /// </summary>
        /// <param name="driver"> Communication group driver that this operator 
        /// should be part of</param>
        /// <param name="masterTaskId">Identifer for MPI master task</param>
        /// <param name="operatorName">Optional parameter to give cutsomized name 
        /// to the operator</param>
        public static void AddFloatArrayMergeReduceOperator(ICommunicationGroupDriver driver, 
            string masterTaskId,
            string operatorName = DefaultCommonOperatorNames.FloatArrayMergeReduceName)
        {
            driver.AddReduce(
                operatorName,
                new ReduceOperatorSpec<float[]>(
                    masterTaskId,
                    new FloatArrayCodec(),
                    new FloatArrayMergeFunction()));
        }

        /// <summary>
        /// Adds double array reduce operator with sum as the function
        /// </summary>
        /// <param name="driver"> Communication group driver that this operator 
        /// should be part of</param>
        /// <param name="masterTaskId">Identifer for MPI master task</param>
        /// <param name="operatorName">Optional parameter to give cutsomized name 
        /// to the operator</param>
        public static void AddDoubleArraySumReduceOperator(ICommunicationGroupDriver driver,
            string masterTaskId,
            string operatorName = DefaultCommonOperatorNames.DoubleArraySumReduceName)
        {
            driver.AddReduce(
                operatorName,
                new ReduceOperatorSpec<double[]>(
                    masterTaskId,
                    new DoubleArrayCodec(),
                    new PipelineDoubleDataConverter(PipelineOptions.ChunkSizeDouble),
                    new DoubleArraySumFunction()));
        }

        /// <summary>
        /// Adds double array reduce operator with merge as the function
        /// The reduce operator in this case will append arrays from each node
        /// in to one big array
        /// </summary>
        /// <param name="driver"> Communication group driver that this operator 
        /// should be part of</param>
        /// <param name="masterTaskId">Identifer for MPI master task</param>
        /// <param name="operatorName">Optional parameter to give cutsomized name 
        /// to the operator</param>
        public static void AddDoubleArrayMergeReduceOperator(ICommunicationGroupDriver driver,
            string masterTaskId,
            string operatorName = DefaultCommonOperatorNames.DoubleArrayMergeReduceName)
        {
            driver.AddReduce(
                operatorName,
                new ReduceOperatorSpec<double[]>(
                    masterTaskId,
                    new DoubleArrayCodec(),
                    new DoubleArrayMergeFunction()));
        }
    }
}
