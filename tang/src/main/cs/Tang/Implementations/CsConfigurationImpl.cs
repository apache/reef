using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Com.Microsoft.Tang.Interface;

namespace Com.Microsoft.Tang.Implementations
{
    public class CsConfigurationImpl : ConfigurationImpl
    {
        public CsConfigurationImpl(CsConfigurationBuilderImpl builder) : base(builder)
        {
        }
    }
}
