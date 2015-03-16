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
using Org.Apache.REEF.Network.Group.Task;
using Org.Apache.REEF.Network.Group.Operators;

namespace Org.Apache.REEF.Network.Group.CommonOperators.Task
{
    public static class GetCommonReduceOperators
    {
        /// <summary>
        /// Gets the integer ReduceSender with give name and sum as the reduce function
        /// </summary>
        /// <param name="client"> Communication group client that this operator 
        /// is part of</param>
        /// <param name="operatorName">Optional customized name of the operator</param>
        /// <returns>The ReduceSender</returns>
        public static IReduceSender<int> GetIntegerSumReduceSenderOperator(ICommunicationGroupClient client,
            string operatorName = DefaultCommonOperatorNames.IntSumReduceName)
        {
            return client.GetReduceSender<int>(operatorName);
        }

        /// <summary>
        /// Gets the integer ReduceReceiver with give name and sum as the reduce function
        /// </summary>
        /// <param name="client"> Communication group client that this operator 
        /// is part of</param>
        /// <param name="operatorName">Optional customized name of the operator</param>
        /// <returns>The ReduceReceiver</returns>
        public static IReduceReceiver<int> GetIntegerSumReduceReceiverOperator(ICommunicationGroupClient client,
            string operatorName = DefaultCommonOperatorNames.IntSumReduceName)
        {
            return client.GetReduceReceiver<int>(operatorName);
        }

        /// <summary>
        /// Gets the integer ReduceSender with give name and min as the reduce function
        /// </summary>
        /// <param name="client"> Communication group client that this operator 
        /// is part of</param>
        /// <param name="operatorName">Optional customized name of the operator</param>
        /// <returns>The ReduceSender</returns>
        public static IReduceSender<int> GetIntegerMinReduceSenderOperator(ICommunicationGroupClient client,
            string operatorName = DefaultCommonOperatorNames.IntMinReduceName)
        {
            return client.GetReduceSender<int>(operatorName);
        }

        /// <summary>
        /// Gets the integer ReduceReceiver with give name and min as the reduce function
        /// </summary>
        /// <param name="client"> Communication group client that this operator 
        /// is part of</param>
        /// <param name="operatorName">Optional customized name of the operator</param>
        /// <returns>The ReduceReceiver</returns>
        public static IReduceReceiver<int> GetIntegerMinReduceReceiverOperator(ICommunicationGroupClient client,
            string operatorName = DefaultCommonOperatorNames.IntMinReduceName)
        {
            return client.GetReduceReceiver<int>(operatorName);
        }

        /// <summary>
        /// Gets the integer ReduceSender with give name and max as the reduce function
        /// </summary>
        /// <param name="client"> Communication group client that this operator 
        /// is part of</param>
        /// <param name="operatorName">Optional customized name of the operator</param>
        /// <returns>The ReduceSender</returns>
        public static IReduceSender<int> GetIntegerMaxReduceSenderOperator(ICommunicationGroupClient client,
            string operatorName = DefaultCommonOperatorNames.IntMaxReduceName)
        {
            return client.GetReduceSender<int>(operatorName);
        }

        /// <summary>
        /// Gets the integer ReduceReceiver with give name and max as the reduce function
        /// </summary>
        /// <param name="client"> Communication group client that this operator 
        /// is part of</param>
        /// <param name="operatorName">Optional customized name of the operator</param>
        /// <returns>The ReduceReceiver</returns>
        public static IReduceReceiver<int> GetIntegerMaxReduceReceiverOperator(ICommunicationGroupClient client,
            string operatorName = DefaultCommonOperatorNames.IntMaxReduceName)
        {
            return client.GetReduceReceiver<int>(operatorName);
        }

        /// <summary>
        /// Gets the float ReduceSender with give name and sum as the reduce function
        /// </summary>
        /// <param name="client"> Communication group client that this operator 
        /// is part of</param>
        /// <param name="operatorName">Optional customized name of the operator</param>
        /// <returns>The ReduceSender</returns>
        public static IReduceSender<float> GetFloatSumReduceSenderOperator(ICommunicationGroupClient client,
            string operatorName = DefaultCommonOperatorNames.FloatSumReduceName)
        {
            return client.GetReduceSender<float>(operatorName);
        }

        /// <summary>
        /// Gets the float ReduceReceiver with give name and sum as the reduce function
        /// </summary>
        /// <param name="client"> Communication group client that this operator 
        /// is part of</param>
        /// <param name="operatorName">Optional customized name of the operator</param>
        /// <returns>The ReduceReceiver</returns>
        public static IReduceReceiver<float> GetFloatSumReduceReceiverOperator(ICommunicationGroupClient client,
            string operatorName = DefaultCommonOperatorNames.FloatSumReduceName)
        {
            return client.GetReduceReceiver<float>(operatorName);
        }

        /// <summary>
        /// Gets the float ReduceSender with give name and min as the reduce function
        /// </summary>
        /// <param name="client"> Communication group client that this operator 
        /// is part of</param>
        /// <param name="operatorName">Optional customized name of the operator</param>
        /// <returns>The ReduceSender</returns>
        public static IReduceSender<float> GetFloatMinReduceSenderOperator(ICommunicationGroupClient client,
            string operatorName = DefaultCommonOperatorNames.FloatMinReduceName)
        {
            return client.GetReduceSender<float>(operatorName);
        }

        /// <summary>
        /// Gets the float ReduceReceiver with give name and min as the reduce function
        /// </summary>
        /// <param name="client"> Communication group client that this operator 
        /// is part of</param>
        /// <param name="operatorName">Optional customized name of the operator</param>
        /// <returns>The ReduceReceiver</returns>
        public static IReduceReceiver<float> GetFloatMinReduceReceiverOperator(ICommunicationGroupClient client,
            string operatorName = DefaultCommonOperatorNames.FloatMinReduceName)
        {
            return client.GetReduceReceiver<float>(operatorName);
        }

        /// <summary>
        /// Gets the float ReduceSender with give name and max as the reduce function
        /// </summary>
        /// <param name="client"> Communication group client that this operator 
        /// is part of</param>
        /// <param name="operatorName">Optional customized name of the operator</param>
        /// <returns>The ReduceSender</returns>
        public static IReduceSender<float> GetFloatMaxReduceSenderOperator(ICommunicationGroupClient client,
            string operatorName = DefaultCommonOperatorNames.FloatMaxReduceName)
        {
            return client.GetReduceSender<float>(operatorName);
        }

        /// <summary>
        /// Gets the float ReduceReceiver with give name and max as the reduce function
        /// </summary>
        /// <param name="client"> Communication group client that this operator 
        /// is part of</param>
        /// <param name="operatorName">Optional customized name of the operator</param>
        /// <returns>The ReduceReceiver</returns>
        public static IReduceReceiver<float> GetFloatMaxReduceReceiverOperator(ICommunicationGroupClient client,
            string operatorName = DefaultCommonOperatorNames.FloatMaxReduceName)
        {
            return client.GetReduceReceiver<float>(operatorName);
        }

        /// <summary>
        /// Gets the double ReduceSender with give name and sum as the reduce function
        /// </summary>
        /// <param name="client"> Communication group client that this operator 
        /// is part of</param>
        /// <param name="operatorName">Optional customized name of the operator</param>
        /// <returns>The ReduceSender</returns>
        public static IReduceSender<double> GetDoubleSumReduceSenderOperator(ICommunicationGroupClient client,
            string operatorName = DefaultCommonOperatorNames.DoubleSumReduceName)
        {
            return client.GetReduceSender<double>(operatorName);
        }

        /// <summary>
        /// Gets the double ReduceReceiver with give name and sum as the reduce function
        /// </summary>
        /// <param name="client"> Communication group client that this operator 
        /// is part of</param>
        /// <param name="operatorName">Optional customized name of the operator</param>
        /// <returns>The ReduceReceiver</returns>
        public static IReduceReceiver<double> GetDoubleSumReduceReceiverOperator(ICommunicationGroupClient client,
            string operatorName = DefaultCommonOperatorNames.DoubleSumReduceName)
        {
            return client.GetReduceReceiver<double>(operatorName);
        }

        /// <summary>
        /// Gets the double ReduceSender with give name and min as the reduce function
        /// </summary>
        /// <param name="client"> Communication group client that this operator 
        /// is part of</param>
        /// <param name="operatorName">Optional customized name of the operator</param>
        /// <returns>The ReduceSender</returns>
        public static IReduceSender<double> GetDoubleMinReduceSenderOperator(ICommunicationGroupClient client,
            string operatorName = DefaultCommonOperatorNames.DoubleMinReduceName)
        {
            return client.GetReduceSender<double>(operatorName);
        }

        /// <summary>
        /// Gets the double ReduceReceiver with give name and min as the reduce function
        /// </summary>
        /// <param name="client"> Communication group client that this operator 
        /// is part of</param>
        /// <param name="operatorName">Optional customized name of the operator</param>
        /// <returns>The ReduceReceiver</returns>
        public static IReduceReceiver<double> GetDoubleMinReduceReceiverOperator(ICommunicationGroupClient client,
            string operatorName = DefaultCommonOperatorNames.DoubleMinReduceName)
        {
            return client.GetReduceReceiver<double>(operatorName);
        }

        /// <summary>
        /// Gets the double ReduceSender with give name and max as the reduce function
        /// </summary>
        /// <param name="client"> Communication group client that this operator 
        /// is part of</param>
        /// <param name="operatorName">Optional customized name of the operator</param>
        /// <returns>The ReduceSender</returns>
        public static IReduceSender<double> GetDoubleMaxReduceSenderOperator(ICommunicationGroupClient client,
            string operatorName = DefaultCommonOperatorNames.DoubleMaxReduceName)
        {
            return client.GetReduceSender<double>(operatorName);
        }

        /// <summary>
        /// Gets the double ReduceReceiver with give name and max as the reduce function
        /// </summary>
        /// <param name="client"> Communication group client that this operator 
        /// is part of</param>
        /// <param name="operatorName">Optional customized name of the operator</param>
        /// <returns>The ReduceReceiver</returns>
        public static IReduceReceiver<double> GetDoubleMaxReduceReceiverOperator(ICommunicationGroupClient client,
            string operatorName = DefaultCommonOperatorNames.DoubleMaxReduceName)
        {
            return client.GetReduceReceiver<double>(operatorName);
        }

        /// <summary>
        /// Gets the integer array ReduceSender with give name and sum as the reduce function
        /// </summary>
        /// <param name="client"> Communication group client that this operator 
        /// is part of</param>
        /// <param name="operatorName">Optional customized name of the operator</param>
        /// <returns>The ReduceSender</returns>
        public static IReduceSender<int[]> GetIntegerArraySumReduceSenderOperator(ICommunicationGroupClient client,
            string operatorName = DefaultCommonOperatorNames.IntArraySumReduceName)
        {
            return client.GetReduceSender<int[]>(operatorName);
        }

        /// <summary>
        /// Gets the integer array ReduceReceiver with give name and sum as the reduce function
        /// </summary>
        /// <param name="client"> Communication group client that this operator 
        /// is part of</param>
        /// <param name="operatorName">Optional customized name of the operator</param>
        /// <returns>The ReduceReceiver</returns>
        public static IReduceReceiver<int[]> GetIntegerArraySumReduceReceiverOperator(ICommunicationGroupClient client,
            string operatorName = DefaultCommonOperatorNames.IntArraySumReduceName)
        {
            return client.GetReduceReceiver<int[]>(operatorName);
        }

        /// <summary>
        /// Gets the integer array ReduceSender with give name and merge/append as the reduce function
        /// </summary>
        /// <param name="client"> Communication group client that this operator 
        /// is part of</param>
        /// <param name="operatorName">Optional customized name of the operator</param>
        /// <returns>The ReduceSender</returns>
        public static IReduceSender<int[]> GetIntegerArrayMergeReduceSenderOperator(ICommunicationGroupClient client,
            string operatorName = DefaultCommonOperatorNames.IntArrayMergeReduceName)
        {
            return client.GetReduceSender<int[]>(operatorName);
        }

        /// <summary>
        /// Gets the integer array ReduceReceiver with give name and merge/append as the reduce function
        /// </summary>
        /// <param name="client"> Communication group client that this operator 
        /// is part of</param>
        /// <param name="operatorName">Optional customized name of the operator</param>
        /// <returns>The ReduceReceiver</returns>
        public static IReduceReceiver<int[]> GetIntegerArrayMergeReduceReceiverOperator(ICommunicationGroupClient client,
            string operatorName = DefaultCommonOperatorNames.IntArrayMergeReduceName)
        {
            return client.GetReduceReceiver<int[]>(operatorName);
        }

        /// <summary>
        /// Gets the float array ReduceSender with give name and sum as the reduce function
        /// </summary>
        /// <param name="client"> Communication group client that this operator 
        /// is part of</param>
        /// <param name="operatorName">Optional customized name of the operator</param>
        /// <returns>The ReduceSender</returns>
        public static IReduceSender<float[]> GetFloatArraySumReduceSenderOperator(ICommunicationGroupClient client,
            string operatorName = DefaultCommonOperatorNames.FloatArraySumReduceName)
        {
            return client.GetReduceSender<float[]>(operatorName);
        }

        /// <summary>
        /// Gets the float array ReduceReceiver with give name and sum as the reduce function
        /// </summary>
        /// <param name="client"> Communication group client that this operator 
        /// is part of</param>
        /// <param name="operatorName">Optional customized name of the operator</param>
        /// <returns>The ReduceReceiver</returns>
        public static IReduceReceiver<float[]> GetFloatArraySumReduceReceiverOperator(ICommunicationGroupClient client,
            string operatorName = DefaultCommonOperatorNames.FloatArraySumReduceName)
        {
            return client.GetReduceReceiver<float[]>(operatorName);
        }

        /// <summary>
        /// Gets the float array ReduceSender with give name and merge/append as the reduce function
        /// </summary>
        /// <param name="client"> Communication group client that this operator 
        /// is part of</param>
        /// <param name="operatorName">Optional customized name of the operator</param>
        /// <returns>The ReduceSender</returns>
        public static IReduceSender<float[]> GetFloatArrayMergeReduceSenderOperator(ICommunicationGroupClient client,
            string operatorName = DefaultCommonOperatorNames.FloatArrayMergeReduceName)
        {
            return client.GetReduceSender<float[]>(operatorName);
        }

        /// <summary>
        /// Gets the float array ReduceReceiver with give name and merge/append as the reduce function
        /// </summary>
        /// <param name="client"> Communication group client that this operator 
        /// is part of</param>
        /// <param name="operatorName">Optional customized name of the operator</param>
        /// <returns>The ReduceReceiver</returns>
        public static IReduceReceiver<float[]> GetFloatArrayMergeReduceReceiverOperator(ICommunicationGroupClient client,
            string operatorName = DefaultCommonOperatorNames.FloatArrayMergeReduceName)
        {
            return client.GetReduceReceiver<float[]>(operatorName);
        }

        /// <summary>
        /// Gets the double array ReduceSender with give name and sum as the reduce function
        /// </summary>
        /// <param name="client"> Communication group client that this operator 
        /// is part of</param>
        /// <param name="operatorName">Optional customized name of the operator</param>
        /// <returns>The ReduceSender</returns>
        public static IReduceSender<double[]> GetDoubleArraySumReduceSenderOperator(ICommunicationGroupClient client,
           string operatorName = DefaultCommonOperatorNames.DoubleArraySumReduceName)
        {
            return client.GetReduceSender<double[]>(operatorName);
        }

        /// <summary>
        /// Gets the double array ReduceReceiver with give name and sum as the reduce function
        /// </summary>
        /// <param name="client"> Communication group client that this operator 
        /// is part of</param>
        /// <param name="operatorName">Optional customized name of the operator</param>
        /// <returns>The ReduceReceiver</returns>
        public static IReduceReceiver<double[]> GetDoubleArraySumReduceReceiverOperator(ICommunicationGroupClient client,
            string operatorName = DefaultCommonOperatorNames.DoubleArraySumReduceName)
        {
            return client.GetReduceReceiver<double[]>(operatorName);
        }

        /// <summary>
        /// Gets the double array ReduceSender with give name and merge/append as the reduce function
        /// </summary>
        /// <param name="client"> Communication group client that this operator 
        /// is part of</param>
        /// <param name="operatorName">Optional customized name of the operator</param>
        /// <returns>The ReduceSender</returns>
        public static IReduceSender<double[]> GetDoubleArrayMergeReduceSenderOperator(ICommunicationGroupClient client,
            string operatorName = DefaultCommonOperatorNames.DoubleArrayMergeReduceName)
        {
            return client.GetReduceSender<double[]>(operatorName);
        }

        /// <summary>
        /// Gets the double array ReduceReceiver with give name and merge/append as the reduce function
        /// </summary>
        /// <param name="client"> Communication group client that this operator 
        /// is part of</param>
        /// <param name="operatorName">Optional customized name of the operator</param>
        /// <returns>The ReduceReceiver</returns>
        public static IReduceReceiver<double[]> GetDoubleArrayMergeReduceReceiverOperator(ICommunicationGroupClient client,
            string operatorName = DefaultCommonOperatorNames.DoubleArrayMergeReduceName)
        {
            return client.GetReduceReceiver<double[]>(operatorName);
        }
    }
}
