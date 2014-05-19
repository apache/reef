#include "InteropUtil.h"
#include "javabridge_NativeInterop.h"
#include "JavaClrBridge.h"
#include "InteropAssemblies.h"
#using "clrhandler.dll" 
#using "Microsoft.Reef.Driver.dll"

using namespace System;
using namespace System::IO;
using namespace System::Collections::Generic;
using namespace System::Runtime::InteropServices;
using namespace System::Reflection;
using namespace Microsoft::Reef::Driver::Bridge;
using namespace Microsoft::Reef::Driver::Evaluator;

namespace Microsoft
{
	namespace Reef
	{
		namespace Driver
		{
			namespace Bridge
			{
				public ref class AllocatedEvaluatorClr2Java : public IAllocatedEvaluaotrClr2Java
				{
					jobject  _jobjectAllocatedEvaluator;
					JavaVM* _jvm;
					jstring _jstringId;
				public:
					AllocatedEvaluatorClr2Java(JNIEnv *env, jobject jallocatedEvaluator);
					virtual void SubmitContextAndTask(String^ contextConfigStr, String^ taskConfigStr);
					virtual void SubmitContext(String^ contextConfigStr);
					virtual void SubmitContextAndService(String^ contextConfigStr, String^ serviceConfigStr);
					virtual void SubmitContextAndServiceAndTask(String^ contextConfigStr, String^ serviceConfigStr, String^ taskConfigStr);
					virtual void Close();
					virtual String^ GetId();
					virtual IEvaluatorDescriptor^ GetEvaluatorDescriptor();
				};

				public ref class ActiveContextClr2Java : public IActiveContextClr2Java
				{
					jobject _jobjectActiveContext;
					jstring _jstringId;
					jstring _jstringEvaluatorId;
					JavaVM* _jvm;
				public:
					ActiveContextClr2Java(JNIEnv *env, jobject jallocatedEvaluator);
					virtual void SubmitTask(String^ taskConfigStr);
					virtual void Close();
					virtual String^ GetId();
					virtual String^ GetEvaluatorId();
					virtual IEvaluatorDescriptor^ GetEvaluatorDescriptor();
				};

				public ref class EvaluatorRequestorClr2Java : public IEvaluatorRequestorClr2Java
				{
					jobject  _jobjectEvaluatorRequestor;
					JavaVM* _jvm;
				public:
					EvaluatorRequestorClr2Java(JNIEnv *env, jobject jevaluatorRequestor);
					virtual void Submit(EvaluatorRequest^ request);
				};

				public ref class TaskMessageClr2Java : public ITaskMessageClr2Java
				{
					jobject  _jobjectTaskMessage;
					JavaVM* _jvm;
				public:
					TaskMessageClr2Java(JNIEnv *env, jobject jtaskMessage);
				};

				public ref class FailedTaskClr2Java : public IFailedTaskClr2Java
				{
					jobject  _jobjectFailedTask;
					JavaVM* _jvm;
				public:
					FailedTaskClr2Java(JNIEnv *env, jobject jfailedTask);
					virtual IActiveContextClr2Java^ GetActiveContext();
					virtual String^ GetString();
				};

				public ref class FailedEvaluatorClr2Java : public IFailedEvaluatorClr2Java
				{
					jobject  _jobjectFailedEvaluator;
					JavaVM* _jvm;
					jstring _jstringId;
				public:
					FailedEvaluatorClr2Java(JNIEnv *env, jobject jfailedEvaluator);
					virtual IEvaluatorRequestorClr2Java^ GetEvaluatorRequestor();
					virtual String^ GetId();
				};
				public ref class HttpServerClr2Java : public IHttpServerBridgeClr2Java
				{
					jlong _handle;
					jobject _jhttpServerEventBridge;
					jobject _jlogger;
					JavaVM* _jvm;
				public:
					HttpServerClr2Java(JNIEnv *env, jlong handle, jobject jhttpServerNRTEventBridge, jobject jlogger);
					virtual String^ GetQueryString();
					virtual void SetUriSpecification(String^ uriSpecification);
//					virtual String^ GetQueryResult();
					virtual void SetQueryResult(String^ queryResult);
				};
			}
		}
	}
}