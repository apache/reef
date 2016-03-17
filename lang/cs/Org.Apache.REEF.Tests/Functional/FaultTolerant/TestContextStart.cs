using System;
using System.Collections.Generic;
using System.Threading;
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Common.Events;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
using Xunit;

namespace Org.Apache.REEF.Tests.Functional.FaultTolerant
{
    /// <summary>
    /// This test case servers as an example to put data downloading at part of the ContextStartHandler
    /// </summary>
    public class TestContextStart : ReefFunctionalTest
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(TestContextStart));
        private const string StartedHandlerMessage = "Start Handler is called.";
        private const string DataDownLoadStartedMessage = "Data Downloading started.";
        private const string DataDownLoadCompletedMessage = "Data download completed.";

        public TestContextStart()
        {
            Init();
        }

        /// <summary>
        /// This test case submit a context with a Context start handler and do something is the handler
        /// </summary>
        [Fact]
        public void TestDosomethingOnContextStartOnLocalRuntime()
        {
            string testFolder = DefaultRuntimeFolder + Guid.NewGuid().ToString("N").Substring(0, 4);
            CleanUp(testFolder);
            TestRun(DriverConfigurations(), typeof(ContextStartDriver), 1, "ContextStartDriver", "local", testFolder);
            ValidateSuccessForLocalRuntime(1, testFolder: testFolder);

            var messages = new List<string>();
            messages.Add(DataDownLoadStartedMessage);
            messages.Add(DataDownLoadCompletedMessage);
            messages.Add(StartedHandlerMessage);
            ValidateMessageSuccessfullyLogged(messages, "Node-*", EvaluatorStdout, testFolder, 1);
            CleanUp(testFolder);
        }

        public IConfiguration DriverConfigurations()
        {
            return DriverConfiguration.ConfigurationModule
                .Set(DriverConfiguration.OnDriverStarted, GenericType<ContextStartDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<ContextStartDriver>.Class)
                .Set(DriverConfiguration.OnContextActive, GenericType<ContextStartDriver>.Class)
                .Build();
        }

        private sealed class ContextStartDriver :
             IObserver<IDriverStarted>,
             IObserver<IAllocatedEvaluator>,
             IObserver<IActiveContext>
        {
            private readonly IEvaluatorRequestor _requestor;

            [Inject]
            private ContextStartDriver(IEvaluatorRequestor evaluatorRequestor)
            {
                _requestor = evaluatorRequestor;
            }

            public void OnNext(IDriverStarted value)
            {
                _requestor.Submit(_requestor.NewBuilder().Build());
            }

            public void OnNext(IActiveContext value)
            {
                var c = TaskConfiguration.ConfigurationModule
                    .Set(TaskConfiguration.Identifier, "TaskID")
                    .Set(TaskConfiguration.Task, GenericType<TestTask>.Class)
                    .Build();
                value.SubmitTask(c);
            }

            public void OnNext(IAllocatedEvaluator value)
            {
                value.SubmitContext(
                    ContextConfiguration.ConfigurationModule
                        .Set(ContextConfiguration.Identifier, "ContextID")
                        .Set(ContextConfiguration.OnContextStart, GenericType<ContextStartHandler>.Class)
                        .Build());
            }

            public void OnError(Exception error)
            {
                throw new NotImplementedException();
            }

            public void OnCompleted()
            {
                throw new NotImplementedException();
            }
        }

        private sealed class ContextStartHandler : IObserver<IContextStart>
        {
            private readonly DataDownLoader _dataDownLoader;

            [Inject]
            private ContextStartHandler(DataDownLoader dataDownLoader)
            {
                _dataDownLoader = dataDownLoader;
            }

            public void OnNext(IContextStart value)
            {
                Logger.Log(Level.Info, StartedHandlerMessage);
                _dataDownLoader.LoadData();
            }

            public void OnError(Exception error)
            {
                throw new NotImplementedException();
            }

            public void OnCompleted()
            {
                throw new NotImplementedException();
            }
        }

        private sealed class DataDownLoader
        {
            private bool _dataLoaded;

            [Inject]
            private DataDownLoader()
            {
                _dataLoaded = false;
            }

            public void LoadData()
            {
                Logger.Log(Level.Info, DataDownLoadStartedMessage);
                _dataLoaded = true;
            }

            public bool DataLoaded
            {
                get { return _dataLoaded; }
            }
        }

        private sealed class TestTask : ITask
        {
            private readonly DataDownLoader _dataDownLoader;

            [Inject]
            private TestTask(DataDownLoader dataDownLoader)
            {
                _dataDownLoader = dataDownLoader;
            }

            public void Dispose()
            {
            }

            public byte[] Call(byte[] memento)
            {
                Logger.Log(Level.Info, "Hello in TestTask");
                if (_dataDownLoader.DataLoaded == true)
                {
                    Logger.Log(Level.Info, DataDownLoadCompletedMessage);
                    return null;
                }
                Thread.Sleep(5 * 1000);
                return null;
            }
        }
    }
}