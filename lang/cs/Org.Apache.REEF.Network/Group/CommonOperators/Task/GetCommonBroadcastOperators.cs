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
    public static class GetCommonBroadcastOperators
    {
        /// <summary>
        /// Gets the integer BroadcastSender with give name
        /// </summary>
        /// <param name="client"> Communication group client that this operator 
        /// is part of</param>
        /// <param name="operatorName">Optional customized name of the operator</param>
        /// <returns>The BroadcastSender</returns>
        public static IBroadcastSender<int> GetIntegerBroadcastSenderOperator(ICommunicationGroupClient client,
            string operatorName = DefaultCommonOperatorNames.IntBroadcastName)
        {
            return client.GetBroadcastSender<int>(operatorName);
        }

        /// <summary>
        /// Gets the integer BroadcastReceiver with give name
        /// </summary>
        /// <param name="client"> Communication group client that this operator 
        /// is part of</param>
        /// <param name="operatorName">Optional customized name of the operator</param>
        /// <returns>The BroadcastReceiver</returns>
        public static IBroadcastReceiver<int> GetIntegerBroadcastReceiverOperator(ICommunicationGroupClient client,
            string operatorName = DefaultCommonOperatorNames.IntBroadcastName)
        {
            return client.GetBroadcastReceiver<int>(operatorName);
        }

        /// <summary>
        /// Gets the float BroadcastSender with give name
        /// </summary>
        /// <param name="client"> Communication group client that this operator 
        /// is part of</param>
        /// <param name="operatorName">Optional customized name of the operator</param>
        /// <returns>The BroadcastSender</returns>
        public static IBroadcastSender<float> GetFloatBroadcastSenderOperator(ICommunicationGroupClient client,
            string operatorName = DefaultCommonOperatorNames.FloatBroadcastName)
        {
            return client.GetBroadcastSender<float>(operatorName);
        }

        /// <summary>
        /// Gets the float BroadcastReceiver with give name
        /// </summary>
        /// <param name="client"> Communication group client that this operator 
        /// is part of</param>
        /// <param name="operatorName">Optional customized name of the operator</param>
        /// <returns>The BroadcastReceiver</returns>
        public static IBroadcastReceiver<float> GetFloatBroadcastReceiverOperator(ICommunicationGroupClient client,
            string operatorName = DefaultCommonOperatorNames.FloatBroadcastName)
        {
            return client.GetBroadcastReceiver<float>(operatorName);
        }

        /// <summary>
        /// Gets the double BroadcastSender with give name
        /// </summary>
        /// <param name="client"> Communication group client that this operator 
        /// is part of</param>
        /// <param name="operatorName">Optional customized name of the operator</param>
        /// <returns>The BroadcastSender</returns>
        public static IBroadcastSender<double> GetDoubleBroadcastSenderOperator(ICommunicationGroupClient client,
            string operatorName = DefaultCommonOperatorNames.DoubleBroadcastName)
        {
            return client.GetBroadcastSender<double>(operatorName);
        }

        /// <summary>
        /// Gets the double BroadcastReceiver with give name
        /// </summary>
        /// <param name="client"> Communication group client that this operator 
        /// is part of</param>
        /// <param name="operatorName">Optional customized name of the operator</param>
        /// <returns>The BroadcastReceiver</returns>
        public static IBroadcastReceiver<double> GetDoubleBroadcastReceiverOperator(ICommunicationGroupClient client,
            string operatorName = DefaultCommonOperatorNames.DoubleBroadcastName)
        {
            return client.GetBroadcastReceiver<double>(operatorName);
        }

        /// <summary>
        /// Gets the integer array BroadcastSender with give name
        /// </summary>
        /// <param name="client"> Communication group client that this operator 
        /// is part of</param>
        /// <param name="operatorName">Optional customized name of the operator</param>
        /// <returns>The BroadcastSender</returns>
        public static IBroadcastSender<int[]> GetIntegerArrayBroadcastSenderOperator(ICommunicationGroupClient client,
            string operatorName = DefaultCommonOperatorNames.IntArrayBroadcastName)
        {
            return client.GetBroadcastSender<int[]>(operatorName);
        }

        /// <summary>
        /// Gets the integer array BroadcastReceiver with give name
        /// </summary>
        /// <param name="client"> Communication group client that this operator 
        /// is part of</param>
        /// <param name="operatorName">Optional customized name of the operator</param>
        /// <returns>The BroadcastReceiver</returns>
        public static IBroadcastReceiver<int[]> GetIntegerArrayBroadcastReceiverOperator(ICommunicationGroupClient client,
            string operatorName = DefaultCommonOperatorNames.IntArrayBroadcastName)
        {
            return client.GetBroadcastReceiver<int[]>(operatorName);
        }

        /// <summary>
        /// Gets the float array BroadcastSender with give name
        /// </summary>
        /// <param name="client"> Communication group client that this operator 
        /// is part of</param>
        /// <param name="operatorName">Optional customized name of the operator</param>
        /// <returns>The BroadcastSender</returns>
        public static IBroadcastSender<float[]> GetFloatArrayBroadcastSenderOperator(ICommunicationGroupClient client,
            string operatorName = DefaultCommonOperatorNames.FloatArrayBroadcastName)
        {
            return client.GetBroadcastSender<float[]>(operatorName);
        }

        /// <summary>
        /// Gets the float array BroadcastReceiver with give name
        /// </summary>
        /// <param name="client"> Communication group client that this operator 
        /// is part of</param>
        /// <param name="operatorName">Optional customized name of the operator</param>
        /// <returns>The BroadcastReceiver</returns>
        public static IBroadcastReceiver<float[]> GetFloatArrayBroadcastReceiverOperator(ICommunicationGroupClient client,
            string operatorName = DefaultCommonOperatorNames.FloatArrayBroadcastName)
        {
            return client.GetBroadcastReceiver<float[]>(operatorName);
        }

        /// <summary>
        /// Gets the double array BroadcastSender with give name
        /// </summary>
        /// <param name="client"> Communication group client that this operator 
        /// is part of</param>
        /// <param name="operatorName">Optional customized name of the operator</param>
        /// <returns>The BroadcastSender</returns>
        public static IBroadcastSender<double[]> GetDoubleArrayBroadcastSenderOperator(ICommunicationGroupClient client,
            string operatorName = DefaultCommonOperatorNames.DoubleArrayBroadcastName)
        {
            return client.GetBroadcastSender<double[]>(operatorName);
        }

        /// <summary>
        /// Gets the double array BroadcastReceiver with give name
        /// </summary>
        /// <param name="client"> Communication group client that this operator 
        /// is part of</param>
        /// <param name="operatorName">Optional customized name of the operator</param>
        /// <returns>The BroadcastReceiver</returns>
        public static IBroadcastReceiver<double[]> GetDoubleArrayBroadcastReceiverOperator(ICommunicationGroupClient client,
            string operatorName = DefaultCommonOperatorNames.DoubleArrayBroadcastName)
        {
            return client.GetBroadcastReceiver<double[]>(operatorName);
        }
    }
}
