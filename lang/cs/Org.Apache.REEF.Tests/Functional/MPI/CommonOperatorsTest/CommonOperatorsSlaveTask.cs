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
using System.Threading;
using System.Linq;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Network.Group.Operators;
using Org.Apache.REEF.Network.Group.Task;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Network.Group.CommonOperators.Task;
using Org.Apache.REEF.Network.Group.CommonOperators;

namespace Org.Apache.REEF.Tests.Functional.MPI.CommonOperatorsTest
{
    public class CommonOperatorsSlaveTask : ITask
    {
        private static readonly Logger _logger = Logger.GetLogger(typeof(CommonOperatorsSlaveTask));

        private readonly int _evaluatorId;
        private readonly IMpiClient _mpiClient;
        private readonly ICommunicationGroupClient _commGroup;

        [Inject]
        public CommonOperatorsSlaveTask(
            [Parameter(typeof(MpiTestConfig.EvaluatorId))] string evaluatorId,
            IMpiClient mpiClient)
        {
            _logger.Log(Level.Info, "Hello from slave task");

            _mpiClient = mpiClient;
            _commGroup = _mpiClient.GetCommunicationGroup(MpiTestConstants.GroupName);

            var stringArr = evaluatorId.Split(new char[] {'-'});
            var length = stringArr.Length;

            if (length < 2)
            {
                throw new Exception();
            }

            _evaluatorId = Convert.ToInt32(stringArr[length-1]);

            _logger.Log(Level.Info, "Evaluator Id " + _evaluatorId);

        }
        public byte[] Call(byte[] memento)
        {
            switch (CommonOperatorsTestConstants.OperatorName)
            {
                case DefaultCommonOperatorNames.DoubleArrayBroadcastName:
                    var doubleArrayBroadcastOperator = GetCommonBroadcastOperators.GetDoubleArrayBroadcastReceiverOperator(_commGroup);
                    double[] doubleArrayResult = doubleArrayBroadcastOperator.Receive();

                    if (doubleArrayResult.Length != CommonOperatorsTestConstants.DoubleArrayData.Length)
                    {
                        throw new Exception("Inside double array broadcast, Expected length " + CommonOperatorsTestConstants.DoubleArrayData.Length + " but got " + doubleArrayResult.Length);
                    }

                    for (int index = 0; index < doubleArrayResult.Length; index++)
                    {
                        if(doubleArrayResult[index] != CommonOperatorsTestConstants.DoubleArrayData[index])
                        {
                            throw new Exception("Inside double array broadcast at index " + index + ", Expected value " + CommonOperatorsTestConstants.DoubleArrayData[index] + " but got " + doubleArrayResult[index]);
                        }
                    }
                    
                    break;

                case DefaultCommonOperatorNames.FloatArrayBroadcastName:
                    var floatArrayBroadcastOperator = GetCommonBroadcastOperators.GetFloatArrayBroadcastReceiverOperator(_commGroup);
                    float[] floatArrayResult = floatArrayBroadcastOperator.Receive();
                    
                    if (floatArrayResult.Length != CommonOperatorsTestConstants.FloatArrayData.Length)
                    {
                        throw new Exception("Inside float array broadcast, Expected length " + CommonOperatorsTestConstants.FloatArrayData.Length + " but got " + floatArrayResult.Length);
                    }

                    for (int index = 0; index < floatArrayResult.Length; index++)
                    {
                        if(floatArrayResult[index] != CommonOperatorsTestConstants.FloatArrayData[index])
                        {
                            throw new Exception("Inside float array broadcast at index " + index + ", Expected value " + CommonOperatorsTestConstants.FloatArrayData[index] + " but got " + floatArrayResult[index]);
                        }
                    }

                    break;

                case DefaultCommonOperatorNames.IntArrayBroadcastName:
                    var intArrayBroadcastOperator = GetCommonBroadcastOperators.GetIntegerArrayBroadcastReceiverOperator(_commGroup);
                    int[] intArrayResult = intArrayBroadcastOperator.Receive();
                    
                    if (intArrayResult.Length != CommonOperatorsTestConstants.IntArrayData.Length)
                    {
                        throw new Exception("Inside int array broadcast, Expected length " + CommonOperatorsTestConstants.IntArrayData.Length + " but got " + intArrayResult.Length);
                    }

                    for (int index = 0; index < intArrayResult.Length; index++)
                    {
                        if(intArrayResult[index] != CommonOperatorsTestConstants.IntArrayData[index])
                        {
                            throw new Exception("Inside int array broadcast at index " + index + ", Expected value " + CommonOperatorsTestConstants.IntArrayData[index] + " but got " + intArrayResult[index]);
                        }
                    }
                    
                    break;

                case DefaultCommonOperatorNames.DoubleBroadcastName:
                    var doubleBroadcastOperator = GetCommonBroadcastOperators.GetDoubleBroadcastReceiverOperator(_commGroup);
                    double doubleNo = doubleBroadcastOperator.Receive();

                    if (doubleNo != CommonOperatorsTestConstants.DoubleNo)
                    {
                        throw new Exception("Inside double broadcast, Expected value " + CommonOperatorsTestConstants.DoubleNo + " but got " + doubleNo);
                    }

                    break;

                case DefaultCommonOperatorNames.IntBroadcastName:
                    var intBroadcastOperator = GetCommonBroadcastOperators.GetIntegerBroadcastReceiverOperator(_commGroup);
                    int intNo = intBroadcastOperator.Receive();

                    if (intNo != CommonOperatorsTestConstants.IntNo)
                    {
                        throw new Exception("Inside int broadcast, Expected value " + CommonOperatorsTestConstants.IntNo + " but got " + intNo);
                    }

                    break;

                case DefaultCommonOperatorNames.FloatBroadcastName:
                    var floatBroadcastOperator = GetCommonBroadcastOperators.GetFloatBroadcastReceiverOperator(_commGroup);
                    double floatNo = floatBroadcastOperator.Receive();

                    if (floatNo != CommonOperatorsTestConstants.FloatNo)
                    {
                        throw new Exception("Inside float broadcast, Expected value " + CommonOperatorsTestConstants.FloatNo + " but got " + floatNo);
                    }

                    break;

                case DefaultCommonOperatorNames.DoubleArrayMergeReduceName:
                    var doubleArrayReduceMergeOperator = GetCommonReduceOperators.GetDoubleArrayMergeReduceSenderOperator(_commGroup);
                    doubleArrayReduceMergeOperator.Send(CommonOperatorsTestConstants.DoubleArrayData);
                    break;

                case DefaultCommonOperatorNames.FloatArrayMergeReduceName:
                    var floatArrayReduceMergeOperator = GetCommonReduceOperators.GetFloatArrayMergeReduceSenderOperator(_commGroup);
                    floatArrayReduceMergeOperator.Send(CommonOperatorsTestConstants.FloatArrayData);
                    break;

                case DefaultCommonOperatorNames.IntArrayMergeReduceName:
                    var integerArrayReduceMergeOperator = GetCommonReduceOperators.GetIntegerArrayMergeReduceSenderOperator(_commGroup);
                    integerArrayReduceMergeOperator.Send(CommonOperatorsTestConstants.IntArrayData);
                    break;

                case DefaultCommonOperatorNames.DoubleArraySumReduceName:
                    var doubleArrayReduceSumOperator = GetCommonReduceOperators.GetDoubleArraySumReduceSenderOperator(_commGroup);
                    doubleArrayReduceSumOperator.Send(CommonOperatorsTestConstants.DoubleArrayData);
                    break;

                case DefaultCommonOperatorNames.FloatArraySumReduceName:
                    var floatArrayReduceSumOperator = GetCommonReduceOperators.GetFloatArraySumReduceSenderOperator(_commGroup);
                    floatArrayReduceSumOperator.Send(CommonOperatorsTestConstants.FloatArrayData);
                    break;

                case DefaultCommonOperatorNames.IntArraySumReduceName:
                    var integerArrayReduceSumOperator = GetCommonReduceOperators.GetIntegerArraySumReduceSenderOperator(_commGroup);
                    integerArrayReduceSumOperator.Send(CommonOperatorsTestConstants.IntArrayData);
                    break;

                case DefaultCommonOperatorNames.DoubleSumReduceName:
                    var doubleReduceSumOperator = GetCommonReduceOperators.GetDoubleSumReduceSenderOperator(_commGroup);
                    doubleReduceSumOperator.Send(CommonOperatorsTestConstants.DoubleNo);
                    break;

                case DefaultCommonOperatorNames.FloatSumReduceName:
                    var floatReduceSumOperator = GetCommonReduceOperators.GetFloatSumReduceSenderOperator(_commGroup);
                    _logger.Log(Level.Info, "$$$$$$$$$$$$$$$$$$ trying to send reduce result $$$$$$$$$$$$$$$ " + CommonOperatorsTestConstants.FloatNo);
                    floatReduceSumOperator.Send(CommonOperatorsTestConstants.FloatNo);
                    _logger.Log(Level.Info, "$$$$$$$$$$$$$$$$$$ done sending  reduce result $$$$$$$$$$$$$$$ " + CommonOperatorsTestConstants.FloatNo);
                    break;

                case DefaultCommonOperatorNames.IntSumReduceName:
                    var integerReduceSumOperator = GetCommonReduceOperators.GetIntegerSumReduceSenderOperator(_commGroup);
                    integerReduceSumOperator.Send(CommonOperatorsTestConstants.IntNo);
                    break;

                case DefaultCommonOperatorNames.DoubleMaxReduceName:
                    var doubleReduceMaxOperator = GetCommonReduceOperators.GetDoubleMaxReduceSenderOperator(_commGroup);
                    doubleReduceMaxOperator.Send(CommonOperatorsTestConstants.DoubleNo * _evaluatorId);
                    break;

                case DefaultCommonOperatorNames.FloatMaxReduceName:
                    var floatReduceMaxOperator = GetCommonReduceOperators.GetFloatMaxReduceSenderOperator(_commGroup);
                    floatReduceMaxOperator.Send(CommonOperatorsTestConstants.FloatNo * _evaluatorId);
                    break;

                case DefaultCommonOperatorNames.IntMaxReduceName:
                    var integerReduceMaxOperator = GetCommonReduceOperators.GetIntegerMaxReduceSenderOperator(_commGroup);
                    integerReduceMaxOperator.Send(CommonOperatorsTestConstants.IntNo * _evaluatorId);
                    break;

                case DefaultCommonOperatorNames.DoubleMinReduceName:
                    var doubleReduceMinOperator = GetCommonReduceOperators.GetDoubleMinReduceSenderOperator(_commGroup);
                    doubleReduceMinOperator.Send(CommonOperatorsTestConstants.DoubleNo * _evaluatorId);
                    break;

                case DefaultCommonOperatorNames.FloatMinReduceName:
                    var floatReduceMinOperator = GetCommonReduceOperators.GetFloatMinReduceSenderOperator(_commGroup);
                    floatReduceMinOperator.Send(CommonOperatorsTestConstants.FloatNo * _evaluatorId);
                    break;

                case DefaultCommonOperatorNames.IntMinReduceName:
                    var integerReduceMinOperator = GetCommonReduceOperators.GetIntegerMinReduceSenderOperator(_commGroup);
                    integerReduceMinOperator.Send(CommonOperatorsTestConstants.IntNo * _evaluatorId);
                    break;
            }

            _logger.Log(Level.Info, "$$$$$$$$$$$$$$$$$$ yp going to sleep $$$$$$$$$$$$$$$ ");

            return null;
        }

        public void Dispose()
        {
            _mpiClient.Dispose();
        }

        private int TriangleNumber(int n)
        {
            return Enumerable.Range(1, n).Sum();
        }
    }
}
