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
    public class CommonOperatorsMasterTask : ITask
    {
        private static readonly Logger _logger = Logger.GetLogger(typeof(CommonOperatorsMasterTask));

        private readonly int _numReduceSenders;

        private readonly IMpiClient _mpiClient;
        private readonly ICommunicationGroupClient _commGroup;

        [Inject]
        public CommonOperatorsMasterTask(
            [Parameter(typeof(MpiTestConfig.NumEvaluators))] int numEvaluators,
            IMpiClient mpiClient)
        {
            _logger.Log(Level.Info, "Hello from master task");
            _numReduceSenders = numEvaluators - 1;
            _mpiClient = mpiClient;
            _commGroup = mpiClient.GetCommunicationGroup(MpiTestConstants.GroupName);
        }

        public byte[] Call(byte[] memento)
        {            
            switch (CommonOperatorsTestConstants.OperatorName)
            {
                case DefaultCommonOperatorNames.DoubleArrayBroadcastName:
                    var doubleArrayBroadcastOperator = GetCommonBroadcastOperators.GetDoubleArrayBroadcastSenderOperator(_commGroup);
                    doubleArrayBroadcastOperator.Send(CommonOperatorsTestConstants.DoubleArrayData);
                    break;

                case DefaultCommonOperatorNames.FloatArrayBroadcastName:
                    var floatArrayBroadcastOperator = GetCommonBroadcastOperators.GetFloatArrayBroadcastSenderOperator(_commGroup);
                    floatArrayBroadcastOperator.Send(CommonOperatorsTestConstants.FloatArrayData);
                    break;

                case DefaultCommonOperatorNames.IntArrayBroadcastName:
                    var intArrayBroadcastOperator = GetCommonBroadcastOperators.GetIntegerArrayBroadcastSenderOperator(_commGroup);
                    intArrayBroadcastOperator.Send(CommonOperatorsTestConstants.IntArrayData);
                    break;

                case DefaultCommonOperatorNames.DoubleBroadcastName:
                    var doubleBroadcastOperator = GetCommonBroadcastOperators.GetDoubleBroadcastSenderOperator(_commGroup);
                    doubleBroadcastOperator.Send(CommonOperatorsTestConstants.DoubleNo);
                    break;

                case DefaultCommonOperatorNames.IntBroadcastName:
                    var intBroadcastOperator = GetCommonBroadcastOperators.GetIntegerBroadcastSenderOperator(_commGroup);
                    intBroadcastOperator.Send(CommonOperatorsTestConstants.IntNo);
                    break;

                case DefaultCommonOperatorNames.FloatBroadcastName:
                    var floatBroadcastOperator = GetCommonBroadcastOperators.GetFloatBroadcastSenderOperator(_commGroup);
                    floatBroadcastOperator.Send(CommonOperatorsTestConstants.FloatNo);
                    break;

                case DefaultCommonOperatorNames.DoubleArrayMergeReduceName:
                    var doubleArrayReduceMergeOperator = GetCommonReduceOperators.GetDoubleArrayMergeReduceReceiverOperator(_commGroup);
                    var mergedDoubleArray = doubleArrayReduceMergeOperator.Reduce();

                    int expectedDoubleArrayLength = _numReduceSenders * CommonOperatorsTestConstants.DoubleArrayData.Length;
                    int slaveDoubleArraySize = CommonOperatorsTestConstants.DoubleArrayData.Length;

                    if (mergedDoubleArray.Length != expectedDoubleArrayLength)
                    {
                        throw new Exception("Inside double array merge/append reduce, Expected length " + expectedDoubleArrayLength + " but got " + mergedDoubleArray.Length);
                    }

                    for (int index = 0; index < expectedDoubleArrayLength; index++)
                    {
                        if (mergedDoubleArray[index] != CommonOperatorsTestConstants.DoubleArrayData[index % slaveDoubleArraySize])
                        {
                            throw new Exception("Inside double array merge/append reduce at index " + index + ", Expected value " + CommonOperatorsTestConstants.DoubleArrayData[index % slaveDoubleArraySize] + " but got " + mergedDoubleArray[index]);
                        }
                    }
                    
                    break;

                case DefaultCommonOperatorNames.FloatArrayMergeReduceName:
                    var floatArrayReduceMergeOperator = GetCommonReduceOperators.GetFloatArrayMergeReduceReceiverOperator(_commGroup);
                    var mergedFloatArray = floatArrayReduceMergeOperator.Reduce();
                    
                    int expectedFloatArrayLength = _numReduceSenders * CommonOperatorsTestConstants.FloatArrayData.Length;
                    int slaveFloatArraySize = CommonOperatorsTestConstants.FloatArrayData.Length;

                    if (mergedFloatArray.Length != expectedFloatArrayLength)
                    {
                        throw new Exception("Inside float array merge/append reduce, Expected length " + expectedFloatArrayLength + " but got " + mergedFloatArray.Length);
                    }

                    for (int index = 0; index < expectedFloatArrayLength; index++)
                    {
                        if (mergedFloatArray[index] != CommonOperatorsTestConstants.FloatArrayData[index % slaveFloatArraySize])
                        {
                            throw new Exception("Inside float array merge/append reduce at index " + index + ", Expected value " + CommonOperatorsTestConstants.FloatArrayData[index % slaveFloatArraySize] + " but got " + mergedFloatArray[index]);
                        }
                    }
                    
                    break;

                case DefaultCommonOperatorNames.IntArrayMergeReduceName:
                    var integerArrayReduceMergeOperator = GetCommonReduceOperators.GetIntegerArrayMergeReduceReceiverOperator(_commGroup);
                    var mergedIntegerArray = integerArrayReduceMergeOperator.Reduce();
                    
                    int expectedIntArrayLength = _numReduceSenders * CommonOperatorsTestConstants.IntArrayData.Length;
                    int slaveIntArraySize = CommonOperatorsTestConstants.IntArrayData.Length;

                    if (mergedIntegerArray.Length != expectedIntArrayLength)
                    {
                        throw new Exception("Inside integer array merge/append reduce, Expected length " + expectedIntArrayLength + " but got " + mergedIntegerArray.Length);
                    }

                    for (int index = 0; index < expectedIntArrayLength; index++)
                    {
                        if (mergedIntegerArray[index] != CommonOperatorsTestConstants.IntArrayData[index % slaveIntArraySize])
                        {
                            throw new Exception("Inside integer array merge/append reduce at index " + index + ", Expected value " + CommonOperatorsTestConstants.IntArrayData[index % slaveIntArraySize] + " but got " + mergedIntegerArray[index]);
                        }
                    }
                    
                    break;

                case DefaultCommonOperatorNames.DoubleArraySumReduceName:
                    var doubleArrayReduceSumOperator = GetCommonReduceOperators.GetDoubleArraySumReduceReceiverOperator(_commGroup);
                    var sumDoubleArray = doubleArrayReduceSumOperator.Reduce();

                    if (sumDoubleArray.Length != CommonOperatorsTestConstants.DoubleArrayData.Length)
                    {
                        throw new Exception("Inside double array sum reduce, Expected length " + CommonOperatorsTestConstants.DoubleArrayData.Length + " but got " + sumDoubleArray.Length);
                    }

                    for (int index = 0; index < sumDoubleArray.Length; index++)
                    {
                        if (sumDoubleArray[index] != _numReduceSenders * CommonOperatorsTestConstants.DoubleArrayData[index])
                        {
                            throw new Exception("Inside double array sum reduce at index " + index + ", Expected value " + CommonOperatorsTestConstants.DoubleArrayData[index] + " but got " + sumDoubleArray[index]);
                        }
                    }
                    
                    break;

                case DefaultCommonOperatorNames.FloatArraySumReduceName:
                    var floatArrayReduceSumOperator = GetCommonReduceOperators.GetFloatArraySumReduceReceiverOperator(_commGroup);
                    var sumFloatArray = floatArrayReduceSumOperator.Reduce();
                    
                    if (sumFloatArray.Length != CommonOperatorsTestConstants.FloatArrayData.Length)
                    {
                        throw new Exception("Inside float array sum reduce, Expected length " + CommonOperatorsTestConstants.FloatArrayData.Length + " but got " + sumFloatArray.Length);
                    }

                    for (int index = 0; index < sumFloatArray.Length; index++)
                    {
                        if(sumFloatArray[index] != _numReduceSenders * CommonOperatorsTestConstants.FloatArrayData[index])
                        {
                            throw new Exception("Inside float array sum reduce at index " + index + ", Expected value " + CommonOperatorsTestConstants.FloatArrayData[index] + " but got " + sumFloatArray[index]);
                        }
                    }
                    
                    break;

                case DefaultCommonOperatorNames.IntArraySumReduceName:
                    var integerArrayReduceSumOperator = GetCommonReduceOperators.GetIntegerArraySumReduceReceiverOperator(_commGroup);
                    var sumIntegerArray = integerArrayReduceSumOperator.Reduce();

                    if (sumIntegerArray.Length != CommonOperatorsTestConstants.IntArrayData.Length)
                    {
                        throw new Exception("Inside int array sum reduce, Expected length " + CommonOperatorsTestConstants.IntArrayData.Length + " but got " + sumIntegerArray.Length);
                    }

                    for (int index = 0; index < sumIntegerArray.Length; index++)
                    {
                        if(sumIntegerArray[index] != _numReduceSenders * CommonOperatorsTestConstants.IntArrayData[index])
                        {
                            throw new Exception("Inside int array sum reduce at index " + index + ", Expected value " + CommonOperatorsTestConstants.IntArrayData[index] + " but got " + sumIntegerArray[index]);
                        }
                    }
                    
                    break;

                case DefaultCommonOperatorNames.DoubleSumReduceName:
                    var doubleReduceSumOperator = GetCommonReduceOperators.GetDoubleSumReduceReceiverOperator(_commGroup);
                    var sumDouble = doubleReduceSumOperator.Reduce();

                    double expectedSumDoubleValue = _numReduceSenders * CommonOperatorsTestConstants.DoubleNo;
                    if (sumDouble != expectedSumDoubleValue)
                    {
                        throw new Exception("Inside double sum reduce, Expected value " + expectedSumDoubleValue + " but got " + sumDouble);
                    }

                    break;

                case DefaultCommonOperatorNames.FloatSumReduceName:
                    var floatReduceSumOperator = GetCommonReduceOperators.GetFloatSumReduceReceiverOperator(_commGroup);
                    _logger.Log(Level.Info, "$$$$$$$$$$$$$$$$$$ trying to get reduce result $$$$$$$$$$$$$$$");
                    var sumFloat = floatReduceSumOperator.Reduce();
                    _logger.Log(Level.Info, "$$$$$$$$$$$$$$$$$$ done getting reduce result $$$$$$$$$$$$$$$ " + sumFloat);
                    
                    float expectedSumFloatValue = _numReduceSenders * CommonOperatorsTestConstants.FloatNo;
                    if (sumFloat != expectedSumFloatValue)
                    {
                        throw new Exception("Inside float sum reduce, Expected value " + expectedSumFloatValue + " but got " + sumFloat);
                    }
                    
                    break;

                case DefaultCommonOperatorNames.IntSumReduceName:
                    var integerReduceSumOperator = GetCommonReduceOperators.GetIntegerSumReduceReceiverOperator(_commGroup);
                    var sumInteger = integerReduceSumOperator.Reduce();
                    
                    int expectedSumIntValue = _numReduceSenders * CommonOperatorsTestConstants.IntNo;
                    if (sumInteger != expectedSumIntValue)
                    {
                        throw new Exception("Inside integer sum reduce, Expected value " + expectedSumIntValue + " but got " + sumInteger);
                    }
                    
                    break;

                case DefaultCommonOperatorNames.DoubleMaxReduceName:
                    var doubleReduceMaxOperator = GetCommonReduceOperators.GetDoubleMaxReduceReceiverOperator(_commGroup);
                    var maxDouble = doubleReduceMaxOperator.Reduce();
                    
                    double expectedDoubleValue = TriangleNumber(_numReduceSenders) * CommonOperatorsTestConstants.DoubleNo;
                    if (maxDouble != expectedDoubleValue)
                    {
                        throw new Exception("Inside doube max reduce, Expected value " + expectedDoubleValue + " but got " + maxDouble);
                    }
                    
                    break;

                case DefaultCommonOperatorNames.FloatMaxReduceName:
                    var floatReduceMaxOperator = GetCommonReduceOperators.GetFloatMaxReduceReceiverOperator(_commGroup);
                    var maxFloat = floatReduceMaxOperator.Reduce();
                    
                    float expectedFloatValue = TriangleNumber(_numReduceSenders) * CommonOperatorsTestConstants.FloatNo;
                    if (maxFloat != expectedFloatValue)
                    {
                        throw new Exception("Inside float max reduce, Expected value " + expectedFloatValue + " but got " + maxFloat);
                    }

                    break;

                case DefaultCommonOperatorNames.IntMaxReduceName:
                    var integerReduceMaxOperator = GetCommonReduceOperators.GetIntegerMaxReduceReceiverOperator(_commGroup);
                    var maxInteger = integerReduceMaxOperator.Reduce();

                    int expectedIntValue = TriangleNumber(_numReduceSenders) * CommonOperatorsTestConstants.IntNo;
                    if (maxInteger != expectedIntValue)
                    {
                        throw new Exception("Inside int max reduce, Expected value " + expectedIntValue + " but got " + maxInteger);
                    }

                    break;

                case DefaultCommonOperatorNames.DoubleMinReduceName:
                    var doubleReduceMinOperator = GetCommonReduceOperators.GetDoubleMinReduceReceiverOperator(_commGroup);
                    var minDouble = doubleReduceMinOperator.Reduce();

                    if (minDouble != CommonOperatorsTestConstants.DoubleNo)
                    {
                        throw new Exception("Inside double min reduce, Expected value " + CommonOperatorsTestConstants.DoubleNo + " but got " + minDouble);
                    }

                    break;

                case DefaultCommonOperatorNames.FloatMinReduceName:
                    var floatReduceMinOperator = GetCommonReduceOperators.GetFloatMinReduceReceiverOperator(_commGroup);
                    var minFloat = floatReduceMinOperator.Reduce();

                    if (minFloat != CommonOperatorsTestConstants.FloatNo)
                    {
                        throw new Exception("Inside float min reduce, Expected value " + CommonOperatorsTestConstants.FloatNo + " but got " + minFloat);
                    }

                    break;

                case DefaultCommonOperatorNames.IntMinReduceName:
                    var integerReduceMinOperator = GetCommonReduceOperators.GetIntegerMinReduceReceiverOperator(_commGroup);
                    var minInteger = integerReduceMinOperator.Reduce();

                    if (minInteger != CommonOperatorsTestConstants.IntNo)
                    {
                        throw new Exception("Inside integer min reduce, Expected value " + CommonOperatorsTestConstants.IntNo + " but got " + minInteger);
                    }
                    break;
            }
           
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
