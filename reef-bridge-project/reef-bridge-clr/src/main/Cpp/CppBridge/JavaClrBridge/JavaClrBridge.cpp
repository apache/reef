#include "InteropUtil.h"
#include "javabridge_NativeInterop.h"
#include "JavaClrBridge.h"
#include "InteropAssemblies.h"
#include "InteropReturnInfo.h"
#include "Clr2JavaImpl.h"

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
	Log0("In c++ JNIEXPORT void JNICALL Java_javabridge_NativeInterop_loadClrAssembly +");
	const wchar_t* charAsmName = UnicodeCppStringFromJavaString (env, jfileName);
	int len = env->GetStringLength(jfileName);	
	
	String^  asmName = Marshal::PtrToStringUni((IntPtr)(unsigned short*) charAsmName, len);	
	System::Reflection::Assembly^ asm1 = Assembly::LoadFrom(asmName);
	AssemblyUtil::Add(asm1);
}

/*
 * Class:     javabridge_NativeInterop
 * Method:    createHandler1
 * Signature: (Ljava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_javabridge_NativeInterop_createHandler1
 (
	JNIEnv *env, 
	jclass  tobj, 
	jstring jstrConfig)
{
	try 
	{
	
	const wchar_t* charConfig = UnicodeCppStringFromJavaString (env, jstrConfig);
	int lenConfig = env->GetStringLength(jstrConfig);		
	String^  strConfig = Marshal::PtrToStringUni((IntPtr)(unsigned short*) charConfig, lenConfig);		
	return ClrHandlerWrapper::CreateFromString_ClrHandler(strConfig);
	}
	catch (System::Exception^ ex)
	{
		Console::WriteLine("Exception in Java_javabridge_NativeInterop_createHandler1");
		Console::WriteLine(ex->Message);
		Console::WriteLine(ex->StackTrace);
	}
	return 0;
}



/*
 * Class:     javabridge_NativeInterop
 * Method:    clrHandlerOnNext
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_javabridge_NativeInterop_clrHandlerOnNext
 (
	JNIEnv *env, 
	jclass  tobj, 
    jlong   handle,
	jbyteArray value)
{
	array<byte>^  bytearray = ManagedByteArrayFromJavaByteArray (env, value);
	ClrHandlerWrapper::CallMethod_ClrHandler_OnNext(handle, bytearray);
}

/*
 * Class:     javabridge_NativeInterop
 * Method:    clrHandlerOnNext2
 * Signature: (Ljavabridge/InteropReturnInfo;J[B)V
 */
JNIEXPORT void JNICALL Java_javabridge_NativeInterop_clrHandlerOnNext2  
(
	JNIEnv *env, 
	jclass  jclassInteropReturnInfo, 
	jobject jObjectInteropReturnInfo,
    jlong   handle,
	jbyteArray value)
{
	InteropReturnInfo^ ret = gcnew InteropReturnInfo(env, jclassInteropReturnInfo, jObjectInteropReturnInfo);
	array<byte>^  bytearray = ManagedByteArrayFromJavaByteArray (env, value);
	ClrHandlerWrapper::CallMethod_ClrHandler_OnNext2(handle, bytearray, ret);
}
/*
 * Class:     javabridge_NativeInterop
 * Method:    CallClrSystemOnStartHandler
 * Signature: (Ljava/lang/String;)V
 */
JNIEXPORT jlong JNICALL Java_javabridge_NativeInterop_CallClrSystemOnStartHandler
  (JNIEnv * env, jclass jclassx, jstring dateTimeString)
{
	try
	{
		
		const wchar_t* charConfig = UnicodeCppStringFromJavaString (env, dateTimeString);
	int lenConfig = env->GetStringLength(dateTimeString);		
	String^  strConfig = Marshal::PtrToStringUni((IntPtr)(unsigned short*) charConfig, lenConfig);		
	Console::WriteLine("Current time is " + strConfig);
	DateTime dt = DateTime::Now; //DateTime::Parse(strConfig);
	return ClrSystemOnStartHandler::OnStart(dt);
	}
	catch (System::Exception^ ex)
	{
		Console::WriteLine("Exception in Java_javabridge_NativeInterop_CallClrSystemOnStartHandler");
		Console::WriteLine(ex->Message);
		Console::WriteLine(ex->StackTrace);
	}
	return 0;
}


/*
 * Class:     javabridge_NativeInterop
 * Method:    CallClrSystemAllocatedEvaluatorHandlerOnNext
 * Signature: (J[B)V
 */
JNIEXPORT void JNICALL Java_javabridge_NativeInterop_CallClrSystemAllocatedEvaluatorHandlerOnNext
  (JNIEnv *env, jclass cls, jlong handle, jobject jobjectEManager, jobject jobject, jbyteArray bytes)
{
	try 
	{
	array<byte>^  bytearray = ManagedByteArrayFromJavaByteArray (env, bytes);
	Clr2JavaImpl^ clr2JavaImpl = gcnew Clr2JavaImpl (env, jobjectEManager);	
	ClrSystemAllocatedEvaluatorHandlerWrapper::CallMethod_ClrSystemAllocatedEvaluatorHandler_OnNext(handle, clr2JavaImpl, bytearray);
	}
	catch (System::Exception^ ex)
	{
		Console::WriteLine("Exception in Java_javabridge_NativeInterop_CallClrSystemOnStartHandler");
		Console::WriteLine(ex->Message);
		Console::WriteLine(ex->StackTrace);
	}
}

