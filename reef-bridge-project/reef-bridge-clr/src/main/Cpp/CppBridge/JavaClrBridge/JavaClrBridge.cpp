#include "InteropUtil.h"
#include "javabridge_NativeInterop.h"
#include "JavaClrBridge.h"
#include "InteropAssemblies.h"
#include "InteropReturnInfo.h"
//#define _MT 1
//#define _DLL 1

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
JNIEXPORT jlong JNICALL Java_com_microsoft_hadoop_mapreduce_lib_aipinput_NativeInterop_createFileReader
  (JNIEnv *env, 
  jclass cls, 
			jstring jfileName, 
		    jstring  jfileStorageAccount,
		    jstring  jfileStorageKey,
		    jstring  jcontainerName,
		    jstring  jdfsHttpAddress,
			jstring  jprotectionServicesLabelsBlobUri,
			jstring  jschemaKind,
			jobject  jinteropLogger,
			jobject  jinteropReturnCode
			)
{		
	
	Log0 ("c++ cfr enter");
	jclass jcinteropLogger = env->GetObjectClass(jinteropLogger);	
	jmethodID jmid = env->GetStaticMethodID(jcinteropLogger, "Trace", "(Ljava/lang/String;I)V");	
	Logger^ logger = gcnew Logger (env, jcinteropLogger, jmid);	
	
	String^ fileName = ManagedStringFromJavaString (env, jfileName);
	String^ fileStorageAccount = ManagedStringFromJavaString (env, jfileStorageAccount);
	String^ fileStorageKey = ManagedStringFromJavaString (env, jfileStorageKey);
	String^ containerName = ManagedStringFromJavaString (env, jcontainerName);
	String^ dfsHttpAddress = ManagedStringFromJavaString (env, jdfsHttpAddress);
	String^ protectionServicesLabelsBlobUri = ManagedStringFromJavaString (env, jprotectionServicesLabelsBlobUri);
	String^ schemaKind = ManagedStringFromJavaString (env, jschemaKind);

	int errorNo = 0;
	String^ exceptionString = nullptr;
	
	unsigned long long cookie  = Datalab::BondIo::GetCookieForFile(
		fileName,
		fileStorageAccount,
		fileStorageKey,
		containerName,
		dfsHttpAddress,
		protectionServicesLabelsBlobUri,
		schemaKind,
		logger,
		errorNo,
		exceptionString
		);
	if (errorNo != 0)
	{
		MarshalErrorToJava (env, jinteropReturnCode, errorNo, exceptionString);
	}

	return cookie;
}

JNIEXPORT jstring JNICALL Java_com_microsoft_hadoop_mapreduce_lib_aipinput_NativeInterop_GetNextRow
(
	JNIEnv*		env, 
	jclass		cls, 
	jlong		cookie,
	jobject		jinteropLogger,
	jobject		jinteropReturnCode
)
{	
	ManagedLog("GetNexRow cookie", cookie.ToString());
	jclass jcinteropLogger = env->GetObjectClass(jinteropLogger);	
	jmethodID jmid = env->GetStaticMethodID(jcinteropLogger, "Trace", "(Ljava/lang/String;I)V");	
	Logger^ logger = gcnew Logger (env, jcinteropLogger, jmid);	

	int errorNo;
	String^ exceptionString = nullptr;

	String^ str = Datalab::BondIo::GetNextRow (
		cookie,
		logger,
		errorNo,
		exceptionString
		);

	if (errorNo != 0)
	{
		MarshalErrorToJava (env, jinteropReturnCode, errorNo, exceptionString);
	}
	
	jstring ret = NULL;
	
	if (!String::IsNullOrEmpty(str))
	{	
		ManagedLog("GetNexRow not null", str);
		pin_ptr<const wchar_t> wch = PtrToStringChars(str);
		Log1(L"Unmanaged GetNexRow not null", wch);
		ret = env->NewString((const jchar*)wch, str->Length);
		if (ret == NULL)
		{
			Log1(L"Ret is NULL", L"xx");
		}	
	}
	else 
	{
		ManagedLog("GetNexRow null", "x");
	}
	return ret;
}
*/
//}
//}
//}