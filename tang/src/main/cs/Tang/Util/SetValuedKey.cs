using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Com.Microsoft.Tang.Util
{
    class SetValuedKey
    {
        public IList<object> key;

        public SetValuedKey(object[] ts, object[] us)
        {
            key = ts.ToList<object>();
            foreach (var o in us)
            {
                key.Add(o);
            }
        }

        public override int GetHashCode()
        {
            int i = 0;
            foreach (object t in key)
            {
                i += t.GetHashCode();
            }
            return i;
        }

        public override bool Equals(Object o)
        {
            SetValuedKey other = (SetValuedKey)o;
            if (other.key.Count != this.key.Count) { return false; }
            for (int i = 0; i < this.key.Count; i++)
            {
                if (this.key[i].Equals(other.key[i]))
                {
                    return false;
                }
            }
            return true;
        }
    }
}
