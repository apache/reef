using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Com.Microsoft.Tang.Annotations;

namespace Com.Microsoft.Tang.Examples
{
    public interface CheckChildIface 
    {
    }   
    
    public class CheckChildImpl : CheckChildIface
    {
        [Inject]
        public CheckChildImpl() {}
    }
}
