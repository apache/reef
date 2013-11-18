using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Com.Microsoft.Tang.Annotations;
using Com.Microsoft.Tang.Implementations;
using Com.Microsoft.Tang.Interface;
using Com.Microsoft.Tang.Types;

namespace Com.Microsoft.Tang.TestDriver
{
    public class TestDriver
    {
        static string file = @"Com.Microsoft.Tang.Examples.dll";

        public static void Main(string[] args)
        {
            EndToEnd();
        }

        private static void EndToEnd()
        {
            var asm = Assembly.LoadFrom(file);
            Type timerType = typeof(Com.Microsoft.Tang.Examples.Timer);
            Type namedParameter = asm.GetType(@"Com.Microsoft.Tang.Examples.Timer+Seconds");

            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder(new string[] { file });
            cb.BindNamedParameter(namedParameter, "2");
            IConfiguration conf = cb.Build();
            IInjector injector = tang.NewInjector(conf);
            var timer = (Com.Microsoft.Tang.Examples.Timer)injector.GetInstance(timerType);
            timer.sleep();
        }

        public static void CreateClassHierarchy()
        {
            ClassHierarchyImpl classHierarchyImpl = new ClassHierarchyImpl(file);
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
    }
 }
