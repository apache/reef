using System.Collections.Generic;

namespace Com.Microsoft.Tang.Types
{
    public interface ITraversable<T> where T : ITraversable<T>
    {
        ICollection<T> GetChildren();
    }
}
