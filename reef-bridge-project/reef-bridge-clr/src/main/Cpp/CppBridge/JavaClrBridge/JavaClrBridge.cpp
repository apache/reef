#include "InteropUtil.h"
#include "javabridge_NativeInterop.h"
#include "JavaClrBridge.h"
#include "InteropAssemblies.h"
#include "InteropReturnInfo.h"
#include "Clr2JavaImpl.h"
#include "InteropLogger.h"

using namespace System;
using namespace System::IO;
using namespace System::Collections::Generic;
using namespace System::Runtime::InteropServices;
using namespace System::Reflection;
using namespace Microsoft::Reef::Interop;

static void ManagedLog (String^ fname, String^ msg)
{		
	Console::WriteLine (fname);
}

static void Log1 (const wchar_t* fname, const wchar_t* msg)
{
	ManagedLog (gcnew String(fname), gcnew String(msg));	
}

static void Log0 (char* fname)
{
	ManagedLog (gcnew String(fname), "");	
}


	
static void MarshalErrorToJava (
		JNIEnv *env, 
		jobject  jerrorInfo,
		int errorNo,
		String^ exceptionString
		)
	{
		jclass objectClass;
		jfieldID fieldID;		

		objectClass = env->GetObjectClass(jerrorInfo);
		fieldID = env->GetFieldID(objectClass, "errorNo", "I");
		env->SetIntField (jerrorInfo, fieldID, errorNo);

		pin_ptr<const wchar_t> wchExceptionString = PtrToStringChars(exceptionString);		
		jstring jexceptionString = env->NewString((const jchar*)wchExceptionString, exceptionString->Length);
		fieldID = env->GetFieldID(objectClass, "exceptionString", "Ljava/lang/String;");
		env->SetObjectField(jerrorInfo, fieldID, jexceptionString);
	}

	
JNIEXPORT void JNICALL Java_javabridge_NativeInterop_loadClrAssembly
  (
	JNIEnv *env, 
	jclass  tobj, 
	jstring jfileName)
{
	const wchar_t* charAsmName = UnicodeCppStringFromJavaString (env, jfileName);
	int len = env->GetStringLength(jfileName);	
	
	String^  asmName = Marshal::PtrToStringUni((IntPtr)(unsigned short*) charAsmName, len);	
	System::Reflection::Assembly^ asm1 = Assembly::LoadFrom(asmName);
	AssemblyUtil::Add(asm1);
}

/*
 * Class:     javabridge_NativeInterop
 * Method:    CallClrSystemOnStartHandler
 * Signature: (Ljava/lang/String;)V
 */
JNIEXPORT jlongArray JNICALL Java_javabridge_NativeInterop_CallClrSystemOnStartHandler
  (JNIEnv * env, jclass jclassx, jstring dateTimeString)
{
	try
	{
		const wchar_t* charConfig = UnicodeCppStringFromJavaString (env, dateTimeString);
		int lenConfig = env->GetStringLength(dateTimeString);		
		String^  strConfig = Marshal::PtrToStringUni((IntPtr)(unsigned short*) charConfig, lenConfig);		
		Console::WriteLine("Current time is " + strConfig);
		DateTime dt = DateTime::Now; 
		array<unsigned long long>^ handlers = ClrSystemHandlerWrapper::Call_ClrSystemStartHandler_OnStart(dt);
		return JavaLongArrayFromManagedLongArray(env, handlers);
	}
	catch (System::Exception^ ex)
	{
		Console::WriteLine("Exceptions in Java_javabridge_NativeInterop_CallClrSystemOnStartHandler");
		Console::WriteLine(ex->Message);
		Console::WriteLine(ex->StackTrace);
	}
	return 0;
}

/*
 * Class:     javabridge_NativeInterop
 * Method:    ClrSystemAllocatedEvaluatorHandlerOnNext
 * Signature: (JLjavabridge/AllocatedEvaluatorBridge;Ljavabridge/InteropLogger;)V
 */
JNIEXPORT void JNICALL Java_javabridge_NativeInterop_ClrSystemAllocatedEvaluatorHandlerOnNext
  (JNIEnv *env, jclass cls, jlong handle, jobject jallocatedEvaluatorBridge, jobject jlogger)
{
	try{
		AllocatedEvaluatorClr2Java^ allocatedEval = gcnew AllocatedEvaluatorClr2Java(env, jallocatedEvaluatorBridge);
		ClrSystemHandlerWrapper::Call_ClrSystemAllocatedEvaluatorHandler_OnNext(handle, allocatedEval);
	}
	catch (System::Exception^ ex)
	{
		Console::WriteLine("Exception in Java_javabridge_NativeInterop_ClrSystemAllocatedEvaluatorHandlerOnNext");
		Console::WriteLine(ex->Message);
		Console::WriteLine(ex->StackTrace);
	}
}

/*
 * Class:     javabridge_NativeInterop
 * Method:    ClrSystemActiveContextHandlerOnNext
 * Signature: (JLjavabridge/ActiveContextBridge;Ljavabridge/InteropLogger;)V
 */
JNIEXPORT void JNICALL Java_javabridge_NativeInterop_ClrSystemActiveContextHandlerOnNext
  (JNIEnv *env, jclass cls, jlong handle, jobject jactiveContextBridge, jobject jlogger)
{
	try{
		ActiveContextClr2Java^ activeContextBrdige = gcnew ActiveContextClr2Java(env, jactiveContextBridge);
		ClrSystemHandlerWrapper::Call_ClrSystemActiveContextHandler_OnNext(handle, activeContextBrdige);
	}
	catch (System::Exception^ ex)
	{
		Console::WriteLine("Exception in Java_javabridge_NativeInterop_ClrSystemActiveContextHandlerOnNext");
		Console::WriteLine(ex->Message);
		Console::WriteLine(ex->StackTrace);
	}
}

/*
 * Class:     javabridge_NativeInterop
 * Method:    ClrSystemEvaluatorRequstorHandlerOnNext
 * Signature: (JLjavabridge/EvaluatorRequstorBridge;Ljavabridge/InteropLogger;)V
 */
JNIEXPORT void JNICALL Java_javabridge_NativeInterop_ClrSystemEvaluatorRequstorHandlerOnNext
  (JNIEnv *env, jclass cls, jlong handle, jobject jevaluatorRequestorBridge, jobject jlogger)
{
	try{
		EvaluatorRequestorClr2Java^ evaluatorRequestorBridge = gcnew EvaluatorRequestorClr2Java(env, jevaluatorRequestorBridge);
		ClrSystemHandlerWrapper::Call_ClrSystemEvaluatorRequestor_OnNext(handle, evaluatorRequestorBridge);
	}
	catch (System::Exception^ ex)
	{
		Console::WriteLine("Exception in Java_javabridge_NativeInterop_ClrSystemEvaluatorRequstorHandlerOnNext");
		Console::WriteLine(ex->Message);
		Console::WriteLine(ex->StackTrace);
	}
}

