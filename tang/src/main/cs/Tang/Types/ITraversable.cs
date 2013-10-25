using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Com.Microsoft.Tang.Types
{
    public interface ITraversable<T> where T : ITraversable<T>
    {
        ICollection<T> GetChildren();
    }
}
