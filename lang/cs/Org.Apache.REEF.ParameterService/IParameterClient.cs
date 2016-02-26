using System;
using System.Security.Cryptography.X509Certificates;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.ParameterService
{
    [DefaultImplementation(typeof(ParameterClient))]
    public interface IParameterClient : IDisposable
    {
        void Add(int tableId, long rowId, float[] pDelta, float coeff);

        void Add(int tableId, long rowId, int[] pDelta, float coeff);

        void AsyncGet(int tableId, long[] rows, float[][] pValues);

        void AsyncGet(int tableId, long[] rows, int[][] pValues);

        void Barrier();

        long BatchLoad(int tableId, long[] rows);

        void Clock();

        void Get(int tableId, long rowId, float[] pValue);

        void Get(int tableId, long rowId, int[] pValue);

        void Set(int tableId, long rowId, float[] pValue);

        void Set(int tableId, long rowId, int[] pValue);
    }
}