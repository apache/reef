using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Org.Apache.REEF.Wake.Remote
{
    /// <summary>
    /// Interface that classes should implement to express their type
    /// </summary>
    public interface IType
    {
        /// <summary>
        /// Returns the type of the class
        /// </summary>
        Type ClassType { get; set; }
    }
}
