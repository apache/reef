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


namespace Org.Apache.REEF.Network.Group.CommonOperators.Driver
{
    /// <summary>
    /// Used to add common broadcast operators on int, float, double and their
    /// arrays by the driver
    /// </summary>
    public static class AddCommonBroadcastOperators
    {
        /// <summary>
        /// Adds integer broadcast operator
        /// </summary>
        /// <param name="driver"> Communication group driver that this operator 
        /// should be part of</param>
        /// <param name="masterTaskId">Identifer for MPI master task</param>
        /// <param name="operatorName">Optional parameter to give cutsomized name 
        /// to the operator</param>
        public static void AddIntegerBroadcastOperator(ICommunicationGroupDriver driver,
            string masterTaskId,
            string operatorName = DefaultCommonOperatorNames.IntBroadcastName)
        {
            driver.AddBroadcast(
                operatorName,
                new BroadcastOperatorSpec<int>(
                    masterTaskId,
                    new IntegerCodec()),
                    TopologyTypes.Flat);
        }

        /// <summary>
        /// Adds float broadcast operator
        /// </summary>
        /// <param name="driver"> Communication group driver that this operator 
        /// should be part of</param>
        /// <param name="masterTaskId">Identifer for MPI master task</param>
        /// <param name="operatorName">Optional parameter to give cutsomized name 
        /// to the operator</param>
        public static void AddFloatBroadcastOperator(ICommunicationGroupDriver driver,
            string masterTaskId,
            string operatorName = DefaultCommonOperatorNames.FloatBroadcastName)
        {
            driver.AddBroadcast(
                operatorName,
                new BroadcastOperatorSpec<float>(
                    masterTaskId,
                    new FloatCodec()),
                    TopologyTypes.Flat);
        }

        /// <summary>
        /// Adds double broadcast operator
        /// </summary>
        /// <param name="driver"> Communication group driver that this operator 
        /// should be part of</param>
        /// <param name="masterTaskId">Identifer for MPI master task</param>
        /// <param name="operatorName">Optional parameter to give cutsomized name 
        /// to the operator</param>
        public static void AddDoubleBroadcastOperator(ICommunicationGroupDriver driver,
            string masterTaskId,
            string operatorName = DefaultCommonOperatorNames.DoubleBroadcastName)
        {
            driver.AddBroadcast(
                operatorName,
                new BroadcastOperatorSpec<double>(
                    masterTaskId,
                    new DoubleCodec()),
                    TopologyTypes.Flat);
        }

        /// <summary>
        /// Adds integer array broadcast operator
        /// </summary>
        /// <param name="driver"> Communication group driver that this operator 
        /// should be part of</param>
        /// <param name="masterTaskId">Identifer for MPI master task</param>
        /// <param name="operatorName">Optional parameter to give cutsomized name 
        /// to the operator</param>
        public static void AddIntegerArrayBroadcastOperator(ICommunicationGroupDriver driver,
            string masterTaskId,
            string operatorName = DefaultCommonOperatorNames.IntArrayBroadcastName)
        {
            driver.AddBroadcast(
                operatorName,
                new BroadcastOperatorSpec<int[]>(
                    masterTaskId,
                    new IntegerArrayCodec(),
                    new PipelineIntDataConverter(PipelineOptions.ChunkSizeInteger)),
                    TopologyTypes.Flat);
        }

        /// <summary>
        /// Adds float array broadcast operator
        /// </summary>
        /// <param name="driver"> Communication group driver that this operator 
        /// should be part of</param>
        /// <param name="masterTaskId">Identifer for MPI master task</param>
        /// <param name="operatorName">Optional parameter to give cutsomized name 
        /// to the operator</param>
        public static void AddFloatArrayBroadcastOperator(ICommunicationGroupDriver driver,
            string masterTaskId,
            string operatorName = DefaultCommonOperatorNames.FloatArrayBroadcastName)
        {
            driver.AddBroadcast(
                operatorName,
                new BroadcastOperatorSpec<float[]>(
                    masterTaskId,
                    new FloatArrayCodec(),
                    new PipelineFloatDataConverter(PipelineOptions.ChunkSizeFloat)),
                    TopologyTypes.Flat);
        }

        /// <summary>
        /// Adds double array broadcast operator
        /// </summary>
        /// <param name="driver"> Communication group driver that this operator 
        /// should be part of</param>
        /// <param name="masterTaskId">Identifer for MPI master task</param>
        /// <param name="operatorName">Optional parameter to give cutsomized name 
        /// to the operator</param>
        public static void AddDoubleArrayBroadcastOperator(ICommunicationGroupDriver driver,
            string masterTaskId,
            string operatorName = DefaultCommonOperatorNames.DoubleArrayBroadcastName)
        {
            driver.AddBroadcast(
                operatorName,
                new BroadcastOperatorSpec<double[]>(
                    masterTaskId,
                    new DoubleArrayCodec(),
                    new PipelineDoubleDataConverter(PipelineOptions.ChunkSizeDouble)),
                    TopologyTypes.Flat);
        }
    }
}
