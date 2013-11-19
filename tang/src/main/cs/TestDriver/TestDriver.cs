using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Com.Microsoft.Tang.Annotations;
using Com.Microsoft.Tang.Implementations;
using Com.Microsoft.Tang.Types;

namespace Com.Microsoft.Tang.TestDriver
{
    public class TestDriver
    {
        public static void Main(string[] args)
        {
//            GetEnclosingClassShortNames("Com.Microsoft.Tang.Examples.B+B1+B2");
  //          GetEnclosingClassShortNames("Com.Microsoft.Tang.Examples.B");
            GetEnclosingClassShortNames("System.IComparable`1[[System.String, mscorlib, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089]]");
            ClassHierarchyImpl classHierarchyImpl = new ClassHierarchyImpl(@"Com.Microsoft.Tang.Examples.dll");
            GetNodeFromHierarchy(classHierarchyImpl);
        }


        public static void GetNodeFromHierarchy(ClassHierarchyImpl classHierarchyImpl)
        {
            IClassNode timerClassNode = (IClassNode)classHierarchyImpl.GetNode("Com.Microsoft.Tang.Examples.Timer");
            INode secondNode = classHierarchyImpl.GetNode("Com.Microsoft.Tang.Examples.Timer+Seconds");

            string classNmae = timerClassNode.GetFullName();
            Type clazz = classHierarchyImpl.assembly.GetType(classNmae);

            IList<IConstructorDef> constuctorDefs = timerClassNode.GetAllConstructors();
            foreach (IConstructorDef consDef in constuctorDefs)
            {
                IList<IConstructorArg> consArgs = consDef.GetArgs();
                foreach (IConstructorArg arg in consArgs)
                {
                    string argName = arg.GetName();
                    string argTypeName = arg.Gettype();
                    Type nt = Type.GetType(argName);
                    INode argNode = classHierarchyImpl.GetNode(nt);
                }
            }
        }


        public static  string[] GetEnclosingClassShortNames(string fullName)
        {
            string[] path = fullName.Split('+');
            string ss = systemName(fullName);


            if (path.Length > 1 || ss == null)
            {
                string[] first = path[0].Split('.');
                path[0] = first[first.Length - 1];
            }
            else
            {
                path[0] = ss;
            }

            return path;
        }

        public static string systemName(string name)
        {
            string[] token = name.Split('[');
            if (token.Length > 1) //system name System.IComparable`1[[System.String, mscorlib, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089]]
            {
                string[] prefixes = token[0].Split('.');
                return prefixes[prefixes.Length - 1];
            }
            return null;
        }
    }
 }
