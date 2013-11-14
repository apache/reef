using System;
using System.Collections.Generic;
using Com.Microsoft.Tang.Exceptions;

namespace Com.Microsoft.Tang.Util
{
    public class MonotonicSet<T> : SortedSet<T>
    {
        private static readonly long serialVersionUID = 1L;
        public MonotonicSet() : base()
        {
        }

        //public MonotonicSet(SortedSet<T> c) : base(c.Comparer)
        //{
        //    AddAll(c);
        //}

        public MonotonicSet(ICollection<T> c)
            : base(c)
        {
        }

        public MonotonicSet(IComparer<T> c)
            : base(c)
        {
        }

        public bool Add(T e)
        {
            if (this.Contains(e))
            {
                throw new ArgumentException("Attempt to re-add " + e
                    + " to MonotonicSet!");
            }
            return base.Add(e);
        }

      
        public bool AddAll(ICollection<T> c) 
        {
            foreach (T t in c)
            {
                this.Add(t);
            }

            return c.Count != 0;
        }

        
        public bool AddAllIgnoreDuplicates(ICollection<T> c) 
        {
            bool ret = false;
            foreach (T t in c) 
            {
                if (!Contains(t)) 
                {
                    Add(t);
                    ret = true;
                }
            }
            return ret;
        }

        public override void Clear() 
        {
            throw new UnsupportedOperationException("Attempt to clear MonotonicSet!");
        }

        public bool Remove(Object o)
        {
            throw new UnsupportedOperationException("Attempt to remove " + o
                + " from MonotonicSet!");
        }
    }
}
