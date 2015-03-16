using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Org.Apache.REEF.Network.Group.CommonOperators
{
    public static class DefaultCommonOperatorNames
    {
        public const string IntBroadcastName = "IntegerBroadcast";
        public const string FloatBroadcastName = "FloatBroadcast";
        public const string DoubleBroadcastName = "DoubleBroadcast";
        
        public const string IntArrayBroadcastName = "IntegerArrayBroadcast";
        public const string FloatArrayBroadcastName = "FloatArrayBroadcast";
        public const string DoubleArrayBroadcastName = "DoubleArrayBroadcast";
        
        public const string IntSumReduceName = "IntegerSumReduce";
        public const string IntMaxReduceName = "IntegerMaxReduce";
        public const string IntMinReduceName = "IntegerMinReduce";
        public const string IntArraySumReduceName = "IntegerArraySumReduce";
        public const string IntArrayMergeReduceName = "IntegerArrayMergeReduce";
        
        public const string FloatSumReduceName = "FloatSumReduce";
        public const string FloatMaxReduceName = "FloatMaxReduce";
        public const string FloatMinReduceName = "FloatMinReduce";
        public const string FloatArraySumReduceName = "FloatArraySumReduce";
        public const string FloatArrayMergeReduceName = "FloatArrayMergeReduce";
        
        public const string DoubleSumReduceName = "DoubleSumReduce";
        public const string DoubleMaxReduceName = "DoubleMaxReduce";
        public const string DoubleMinReduceName = "DoubleMinReduce";
        public const string DoubleArraySumReduceName = "DoubleArraySumReduce";
        public const string DoubleArrayMergeReduceName = "DoubleArrayMergeReduce";

    }
}
