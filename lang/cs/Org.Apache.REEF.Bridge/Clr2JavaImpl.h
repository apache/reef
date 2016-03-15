// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "InteropUtil.h"
#include "org_apache_reef_javabridge_NativeInterop.h"
#include "JavaClrBridge.h"
#include "InteropAssemblies.h"
#using "Org.Apache.REEF.Driver.dll"

using namespace System;
using namespace System::IO;
using namespace System::Collections::Generic;
using namespace System::Runtime::InteropServices;
using namespace System::Reflection;
using namespace Org::Apache::REEF::Driver::Bridge;
using namespace Org::Apache::REEF::Driver::Evaluator;

namespace Org {
    namespace Apache {
        namespace REEF {
            namespace Driver {
                namespace Bridge {
                    namespace Clr2java {
                        public ref class CommonUtilities {
                        public:
                            static IEvaluatorDescriptor^ RetrieveEvaluatorDescriptor(jobject object, JavaVM* jvm);
                            template<typename T> static T CallGetMethodNewGlobalRef(JNIEnv* env, jobject obj, jmethodID jmid) {
                                return reinterpret_cast<T>(env->NewGlobalRef(env->CallObjectMethod(obj, jmid)));
                            }

                            static jstring GetJObjectId(JNIEnv* env, jobject obj, jclass javaClass) {
                                jmethodID jmid = env->GetMethodID(javaClass, "getId", "()Ljava/lang/String;");
                                return CommonUtilities::CallGetMethodNewGlobalRef<jstring>(env, obj, jmid);
                            };

                            static jstring GetJObjectEvaluatorId(JNIEnv* env, jobject obj, jclass javaClass) {
                                jmethodID jmid = env->GetMethodID(javaClass, "getEvaluatorId", "()Ljava/lang/String;");
                                return CommonUtilities::CallGetMethodNewGlobalRef<jstring>(env, obj, jmid);
                            };
                        };

                        public ref class AllocatedEvaluatorClr2Java : public IAllocatedEvaluatorClr2Java {
                            jobject  _jobjectAllocatedEvaluator = NULL;
                            JavaVM* _jvm;
                            jstring _jstringId = NULL;
                            jstring _jstringNameServerInfo = NULL;
                        public:
                            AllocatedEvaluatorClr2Java(JNIEnv *env, jobject jallocatedEvaluator);
                            ~AllocatedEvaluatorClr2Java();
                            !AllocatedEvaluatorClr2Java();
                            virtual void SubmitContextAndTask(String^ contextConfigStr, String^ taskConfigStr);
                            virtual void SubmitContext(String^ contextConfigStr);
                            virtual void SubmitContextAndService(String^ contextConfigStr, String^ serviceConfigStr);
                            virtual void SubmitContextAndServiceAndTask(String^ contextConfigStr, String^ serviceConfigStr, String^ taskConfigStr);
                            virtual void OnError(String^ message);
                            virtual void Close();
                            virtual String^ GetId();
                            virtual String^ GetNameServerInfo();
                            virtual IEvaluatorDescriptor^ GetEvaluatorDescriptor();
                        };

                        public ref class ActiveContextClr2Java : public IActiveContextClr2Java {
                            jobject _jobjectActiveContext = NULL;
                            jstring _jstringId = NULL;
                            jstring _jstringParentId = NULL;
                            jstring _jstringEvaluatorId = NULL;
                            JavaVM* _jvm;
                        public:
                            ActiveContextClr2Java(JNIEnv *env, jobject jallocatedEvaluator);
                            ~ActiveContextClr2Java();
                            !ActiveContextClr2Java();
                            virtual void SubmitTask(String^ taskConfigStr);
                            virtual void SubmitContext(String^ contextConfigStr);
                            virtual void Close();
                            virtual void OnError(String^ message);
                            virtual String^ GetParentId();
                            virtual String^ GetId();
                            virtual String^ GetEvaluatorId();
                            virtual IEvaluatorDescriptor^ GetEvaluatorDescriptor();
                            virtual void SendMessage(array<byte>^ message);
                        };

                        public ref class EvaluatorRequestorClr2Java : public IEvaluatorRequestorClr2Java {
                            jobject  _jobjectEvaluatorRequestor = NULL;
                            JavaVM* _jvm;
                        public:
                            EvaluatorRequestorClr2Java(JNIEnv *env, jobject jevaluatorRequestor);
                            ~EvaluatorRequestorClr2Java();
                            !EvaluatorRequestorClr2Java();
                            virtual void OnError(String^ message);
                            virtual void Submit(IEvaluatorRequest^ request);
                        };

                        public ref class TaskMessageClr2Java : public ITaskMessageClr2Java {
                            jobject  _jobjectTaskMessage = NULL;
                            JavaVM* _jvm;
                            jstring _jstringId = NULL;
                        public:
                            TaskMessageClr2Java(JNIEnv *env, jobject jtaskMessage);
                            ~TaskMessageClr2Java();
                            !TaskMessageClr2Java();
                            virtual void OnError(String^ message);
                            virtual String^ GetId();
                        };

                        public ref class FailedTaskClr2Java : public IFailedTaskClr2Java {
                            jobject  _jobjectFailedTask = NULL;
                            JavaVM* _jvm;
                        public:
                            FailedTaskClr2Java(JNIEnv *env, jobject jfailedTask);
                            ~FailedTaskClr2Java();
                            !FailedTaskClr2Java();
                            virtual void OnError(String^ message);
                            virtual IActiveContextClr2Java^ GetActiveContext();
                            virtual String^ GetString();
                        };

                        public ref class RunningTaskClr2Java : public IRunningTaskClr2Java {
                            jobject  _jobjectRunningTask = NULL;
                            JavaVM* _jvm;
                            jstring _jstringId = NULL;
                        public:
                            RunningTaskClr2Java(JNIEnv *env, jobject jrunningTask);
                            ~RunningTaskClr2Java();
                            !RunningTaskClr2Java();
                            virtual void OnError(String^ message);
                            virtual IActiveContextClr2Java^ GetActiveContext();
                            virtual String^ GetId();
                            virtual void Send(array<byte>^ message);
                            virtual void Suspend(array<byte>^ message);
                            virtual void Close(array<byte>^ message);
                        };

                        public ref class FailedEvaluatorClr2Java : public IFailedEvaluatorClr2Java {
                            jobject  _jobjectFailedEvaluator = NULL;
                            JavaVM* _jvm;
                            jstring _jstringId = NULL;
                        public:
                            FailedEvaluatorClr2Java(JNIEnv *env, jobject jfailedEvaluator);
                            ~FailedEvaluatorClr2Java();
                            !FailedEvaluatorClr2Java();
                            virtual void OnError(String^ message);
                            virtual IEvaluatorRequestorClr2Java^ GetEvaluatorRequestor();
                            virtual String^ GetId();
                            virtual EvaluatorException^ GetException();
                            virtual array<IFailedContextClr2Java^>^ GetFailedContextsClr2Java();
                            virtual IFailedTaskClr2Java^ GetFailedTaskClr2Java();
                        private:
                            String^ GetCause();
                            String^ GetStackTrace();
                        };

                        public ref class HttpServerClr2Java : public IHttpServerBridgeClr2Java {
                            jobject _jhttpServerEventBridge = NULL;
                            JavaVM* _jvm;
                        public:
                            HttpServerClr2Java(JNIEnv *env, jobject jhttpServerEventBridge);
                            ~HttpServerClr2Java();
                            !HttpServerClr2Java();
                            virtual void OnError(String^ message);
                            virtual String^ GetQueryString();
                            virtual void SetUriSpecification(String^ uriSpecification);
                            virtual void SetQueryResult(String^ queryResult);
                            virtual array<byte>^ GetQueryRequestData();
                            virtual void SetQueryResponseData(array<byte>^ responseData);
                        };

                        public ref class CompletedTaskClr2Java : public ICompletedTaskClr2Java {
                            jobject  _jobjectCompletedTask = NULL;
                            JavaVM* _jvm;
                            jstring _jstringId = NULL;
                        public:
                            CompletedTaskClr2Java(JNIEnv *env, jobject jcompletedTask);
                            ~CompletedTaskClr2Java();
                            !CompletedTaskClr2Java();
                            virtual void OnError(String^ message);
                            virtual IActiveContextClr2Java^ GetActiveContext();
                            virtual String^ GetId();
                            virtual array<byte>^ Get();
                        };

                        public ref class SuspendedTaskClr2Java : public ISuspendedTaskClr2Java {
                            jobject  _jobjectSuspendedTask = NULL;
                            JavaVM* _jvm;
                            jstring _jstringId = NULL;
                        public:
                            SuspendedTaskClr2Java(JNIEnv *env, jobject jobjectSuspendedTask);
                            ~SuspendedTaskClr2Java();
                            !SuspendedTaskClr2Java();
                            virtual void OnError(String^ message);
                            virtual IActiveContextClr2Java^ GetActiveContext();
                            virtual String^ GetId();
                            virtual array<byte>^ Get();
                        };

                        public ref class CompletedEvaluatorClr2Java : public ICompletedEvaluatorClr2Java {
                            jobject  _jobjectCompletedEvaluator = NULL;
                            JavaVM* _jvm;
                            jstring _jstringId = NULL;
                        public:
                            CompletedEvaluatorClr2Java(JNIEnv *env, jobject jobjectCompletedEvaluator);
                            ~CompletedEvaluatorClr2Java();
                            !CompletedEvaluatorClr2Java();
                            virtual void OnError(String^ message);
                            virtual String^ GetId();
                        };

                        public ref class ClosedContextClr2Java : public IClosedContextClr2Java {
                            jobject  _jobjectClosedContext = NULL;
                            JavaVM* _jvm;
                            jstring _jstringContextId = NULL;
                            jstring _jstringEvaluatorId = NULL;
                            ActiveContextClr2Java^ _parentContext = nullptr;
                        public:
                            ClosedContextClr2Java(JNIEnv *env, jobject jobjectClosedContext);
                            ~ClosedContextClr2Java();
                            !ClosedContextClr2Java();
                            virtual void OnError(String^ message);
                            virtual String^ GetId();
                            virtual String^ GetEvaluatorId();
                            virtual IEvaluatorDescriptor^ GetEvaluatorDescriptor();
                            virtual IActiveContextClr2Java^ GetParentContext();
                        };

                        public ref class FailedContextClr2Java : public IFailedContextClr2Java {
                            jobject  _jobjectFailedContext = NULL;
                            JavaVM* _jvm;
                            jstring _jstringContextId = NULL;
                            jstring _jstringEvaluatorId = NULL;
                            jstring _jstringParentContextId = NULL;
                        public:
                            FailedContextClr2Java(JNIEnv *env, jobject jobjectFailedContext);
                            ~FailedContextClr2Java();
                            !FailedContextClr2Java();
                            virtual void OnError(String^ message);
                            virtual String^ GetId();
                            virtual String^ GetEvaluatorId();
                            virtual String^ GetParentId();
                            virtual IEvaluatorDescriptor^ GetEvaluatorDescriptor();
                            virtual IActiveContextClr2Java^ GetParentContext();
                        };

                        public ref class ContextMessageClr2Java : public IContextMessageClr2Java {
                            jobject  _jobjectContextMessage = NULL;
                            JavaVM* _jvm;
                            jbyteArray _jarrayMessage = NULL;
                            jstring _jstringId = NULL;
                            jstring _jstringSourceId = NULL;
                        public:
                            ContextMessageClr2Java(JNIEnv *env, jobject jobjectContextMessage);
                            ~ContextMessageClr2Java();
                            !ContextMessageClr2Java();
                            virtual void OnError(String^ message);
                            virtual array<byte>^ Get();
                            virtual String^ GetId();
                            virtual String^ GetMessageSourceId();
                        };

                        public ref class DriverRestartedClr2Java : public IDriverRestartedClr2Java {
                            jobject _jobjectDriverRestarted = NULL;
                            JavaVM* _jvm;
                            array<String^>^ _expectedEvaluatorIds;
                            DateTime _startTime;
                            int _resubmissionAttempts;
                        public:
                            DriverRestartedClr2Java(JNIEnv *env, jobject jobjectDriverRestarted);
                            ~DriverRestartedClr2Java();
                            !DriverRestartedClr2Java();
                            virtual void OnError(String^ message);
                            virtual array<String^>^ GetExpectedEvaluatorIds();
                            virtual DateTime GetStartTime();
                            virtual int GetResubmissionAttempts();
                        };

                        public ref class DriverRestartCompletedClr2Java : public IDriverRestartCompletedClr2Java {
                            jobject _jobjectDriverRestartCompleted = NULL;
                            JavaVM* _jvm;
                            DateTime _restartCompletedTime;
                            bool _isTimedOut;
                        public:
                            DriverRestartCompletedClr2Java(JNIEnv *env, jobject jobobjectDriverRestartCompleted);
                            ~DriverRestartCompletedClr2Java();
                            !DriverRestartCompletedClr2Java();
                            virtual void OnError(String^ message);
                            virtual DateTime GetCompletedTime();
                            virtual bool IsTimedOut();
                        };
                    }
                }
            }
        }
    }
}