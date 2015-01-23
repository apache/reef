using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Org.Apache.Reef.IO.Network.Group.Operators
{
    public class ReduceFunction<T> : IReduceFunction<T>
    {
        private Func<T, T, T> _reduceFunction;
        private T _initialValue;
 
        private ReduceFunction(Func<T, T, T> reduceFunction)
        {
            _reduceFunction = reduceFunction;
        }

        private ReduceFunction(Func<T, T, T> reduceFunction, T initialValue)
        {
            _reduceFunction = reduceFunction;
            _initialValue = initialValue;
        }

        public static IReduceFunction<T> Create(Func<T, T, T> reduceFunction)
        {
            return new ReduceFunction<T>(reduceFunction);
        }

        public static IReduceFunction<T> Create(Func<T, T, T> reduceFunction, T initialValue)
        {
            return new ReduceFunction<T>(reduceFunction, initialValue);
        }

        public T Reduce(IEnumerable<T> elements)
        {
            if (_initialValue == null)
            {
                return elements.Aggregate(_reduceFunction);
            }

            return elements.Aggregate(_initialValue, _reduceFunction);
        }
    }
}
