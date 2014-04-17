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
					jclass  _jclassAllocatedEvaluator;
					jobject  _jobjectAllocatedEvaluator;
					jstring _contextConfigStr;
					jstring _taskConfigStr;
					jmethodID _jmidSubmitContextAndTask;
					jmethodID _jmidSubmitContext;
					jmethodID _jmidSubmitContextAndService;
					jmethodID _jmidSubmitContextAndServiceAndTask;
					JNIEnv* _env;
				public:
					AllocatedEvaluatorClr2Java(JNIEnv *env, jobject jallocatedEvaluator);
					virtual void SubmitContextAndTask(String^ contextConfigStr, String^ taskConfigStr);
					virtual void SubmitContext(String^ contextConfigStr);
					virtual void SubmitContextAndService(String^ contextConfigStr, String^ serviceConfigStr);
					virtual void SubmitContextAndServiceAndTask(String^ contextConfigStr, String^ serviceConfigStr, String^ taskConfigStr);
				};

				public ref class ActiveContextClr2Java : public IActiveContextClr2Java
				{
					jclass  _jclassActiveContext;
					jobject  _jobjectActiveContext;
					jstring _taskConfigStr;
					jmethodID _jmidSubmitTask;
					JNIEnv* _env;
				public:
					ActiveContextClr2Java(JNIEnv *env, jobject jallocatedEvaluator);
					virtual void SubmitTask(String^ taskConfigStr);
				};

				public ref class EvaluatorRequestorClr2Java : public IEvaluatorRequestorClr2Java
				{
					jclass  _jclassEvaluatorRequestor;
					jobject  _jobjectEvaluatorRequestor;
					EvaluatorRequest _request;
					jmethodID _jmidSubmit;
					JNIEnv* _env;
				public:
					EvaluatorRequestorClr2Java(JNIEnv *env, jobject jevaluatorRequestor);
					virtual void Submit(EvaluatorRequest^ request);
				};

				public ref class TaskMessageClr2Java : public ITaskMessageClr2Java
				{
					jclass  _jclassTaskMessage;
					jobject  _jobjectTaskMessage;
					JNIEnv* _env;
				public:
					TaskMessageClr2Java(JNIEnv *env, jobject jtaskMessage);
				};
			}
		}
	}
}