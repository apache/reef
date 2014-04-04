using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Reef.Interop
{
    public interface IActiveContextClr2Java
    {
        void SubmitTask(string taskConfigStr);
    }
}
