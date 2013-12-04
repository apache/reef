using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Com.Microsoft.Tang.Annotations;
using Com.Microsoft.Tang.Implementations;
using Com.Microsoft.Tang.Interface;

namespace Com.Microsoft.Tang.Examples
{
    public class ForksInjectorInConstructor
    {
        [Inject]
        public ForksInjectorInConstructor(IInjector i)
        {
            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder(new string[] { @"Com.Microsoft.Tang.Examples" });
            //cb.BindImplementation(Number.class, typeof(Int32));
            i.ForkInjector(new IConfiguration[] { cb.Build() });
        }
    }
}
