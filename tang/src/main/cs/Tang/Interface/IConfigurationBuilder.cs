using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Com.Microsoft.Tang.Types;

namespace Com.Microsoft.Tang.Interface
{
    public interface IConfigurationBuilder
    {
        void AddConfiguration(IConfiguration c); 
        IClassHierarchy GetClassHierarchy();
        IConfiguration Build();
        void Bind(string iface, string impl);
        void Bind(INode key, INode value);

        void BindConstructor(IClassNode k, IClassNode v); //v extended from ExternalConstructor
        string ClassPrettyDefaultString(string longName);
        string ClassPrettyDescriptionString(string longName);


    }
}
