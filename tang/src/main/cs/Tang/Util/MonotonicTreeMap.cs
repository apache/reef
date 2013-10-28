using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Com.Microsoft.Tang.Util
{
    public class MonotonicTreeMap<TKey, TVal> : SortedDictionary<TKey, TVal> 
    {
        public void Add(TKey key, TVal value) 
        {
            TVal val;
            if (base.TryGetValue(key, out val))
            {
                    throw new ArgumentException("Attempt to re-add: [" + key
                    + "]\n old value: " + val + " new value " + value);
            }
            else
            {
                base.Add(key, value);
            }
        }

        public void Clear() 
        {
            throw new System.NotSupportedException();
        }

        public void Remove(TKey key)
        {
            throw new NotSupportedException();
        }
    }
}
