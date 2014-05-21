#include "InteropUtil.h"
#include "javabridge_NativeInterop.h"
#include "JavaClrBridge.h"
#include "InteropAssemblies.h"
#include "InteropReturnInfo.h"
#include "Clr2JavaImpl.h"
#include "InteropLogger.h"
#include "BinaryUtil.h"
#include "malloc.h"

using namespace System;
using namespace System::IO;
using namespace System::Collections::Generic;
using namespace System::Runtime::InteropServices;
using namespace System::Reflection;
using namespace Microsoft::Reef::Driver::Bridge;
using namespace Microsoft::Reef::Utilities::Logging;

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
	try
	{
		Console::Write("+Java_javabridge_NativeInterop_loadClrAssembly: ");
		const wchar_t* charAsmName = UnicodeCppStringFromJavaString (env, jfileName);
		int len = env->GetStringLength(jfileName);	
		wchar_t* fileName = (wchar_t* )_alloca((len+2)* sizeof(wchar_t));
		memcpy(fileName, charAsmName, (len+2)* sizeof(wchar_t));
		fileName[len] = 0;
		String^ asmName = ManagedStringFromJavaString(env, jfileName);
		Console::WriteLine(asmName);

		BINARY_TYPE binaryType = IsManagedBinary(fileName);
		if (binaryType == BINARY_TYPE_CLR)
		{
			System::Reflection::Assembly^ asm1 = Assembly::LoadFrom(asmName);
			AssemblyUtil::Add(asm1);
		}
		else if (binaryType == BINARY_TYPE_NATIVE)
		{
			HANDLE handle = LoadLibraryW(fileName);
		}
	}
	catch (System::Exception^ ex)
	{
		Console::WriteLine("Exceptions in Java_javabridge_NativeInterop_loadClrAssembly");
		Console::WriteLine(ex->Message);
		Console::WriteLine(ex->StackTrace);
	}

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
		Console::WriteLine("+Java_javabridge_NativeInterop_CallClrSystemOnStartHandler");
		const wchar_t* charConfig = UnicodeCppStringFromJavaString (env, dateTimeString);
		int lenConfig = env->GetStringLength(dateTimeString);		
		String^  strConfig = Marshal::PtrToStringUni((IntPtr)(unsigned short*) charConfig, lenConfig);		
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
		Console::WriteLine("+Java_javabridge_NativeInterop_ClrSystemAllocatedEvaluatorHandlerOnNext");
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
		Console::WriteLine("+Java_javabridge_NativeInterop_ClrSystemActiveContextHandlerOnNext");
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
		Console::WriteLine("+Java_javabridge_NativeInterop_ClrSystemEvaluatorRequstorHandlerOnNext");
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

/*
 * Class:     javabridge_NativeInterop
 * Method:    ClrSystemTaskMessageHandlerOnNext
 * Signature: (J[BLjavabridge/TaskMessageBridge;Ljavabridge/InteropLogger;)V
 */
JNIEXPORT void JNICALL Java_javabridge_NativeInterop_ClrSystemTaskMessageHandlerOnNext
	(JNIEnv *env, jclass cls, jlong handle, jbyteArray jmessage, jobject jtaskMessageBridge, jobject jlogger)
{
	try{
		Console::WriteLine("+Java_javabridge_NativeInterop_ClrSystemTaskMessageHandlerOnNext");
		TaskMessageClr2Java^ taskMesageBridge = gcnew TaskMessageClr2Java(env, jtaskMessageBridge);
		array<byte>^ message = ManagedByteArrayFromJavaByteArray(env, jmessage);
		ClrSystemHandlerWrapper::Call_ClrSystemTaskMessage_OnNext(handle, taskMesageBridge, message);
	}
	catch (System::Exception^ ex)
	{
		Console::WriteLine("Exception in Java_javabridge_NativeInterop_ClrSystemTaskMessageHandlerOnNext");
		Console::WriteLine(ex->Message);
		Console::WriteLine(ex->StackTrace);
	}
}

/*
 * Class:     javabridge_NativeInterop
 * Method:    ClrSysteFailedTaskHandlerOnNext
 * Signature: (JLjavabridge/FailedTaskBridge;Ljavabridge/InteropLogger;)V
 */
JNIEXPORT void JNICALL Java_javabridge_NativeInterop_ClrSystemFailedTaskHandlerOnNext
  (JNIEnv *env , jclass cls, jlong handler, jobject jfailedTask, jobject jlogger)
{
	try{
		Console::WriteLine("+Java_javabridge_NativeInterop_ClrSystemFailedTaskHandlerOnNext");
		FailedTaskClr2Java^ failedTaskBridge = gcnew FailedTaskClr2Java(env, jfailedTask);
		ClrSystemHandlerWrapper::Call_ClrSystemFailedTask_OnNext(handler, failedTaskBridge);
	}
	catch (System::Exception^ ex)
	{
		Console::WriteLine("Exception in Java_javabridge_NativeInterop_ClrSysteFailedTaskHandlerOnNext");
		Console::WriteLine(ex->Message);
		Console::WriteLine(ex->StackTrace);
	}
}

/*
 * Class:     javabridge_NativeInterop
 * Method:    ClrSystemFailedEvaluatorHandlerOnNext
 * Signature: (JLjavabridge/FailedEvaluatorBridge;Ljavabridge/InteropLogger;)V
 */
JNIEXPORT void JNICALL Java_javabridge_NativeInterop_ClrSystemFailedEvaluatorHandlerOnNext
  (JNIEnv *env , jclass cls, jlong handler, jobject jfailedEvaluator, jobject jlogger)
{
	try{
		Console::WriteLine("+Java_javabridge_NativeInterop_ClrSystemFailedEvaluatorHandlerOnNext");
		FailedEvaluatorClr2Java^ failedEvaluatorBridge = gcnew FailedEvaluatorClr2Java(env, jfailedEvaluator);
		ClrSystemHandlerWrapper::Call_ClrSystemFailedEvaluator_OnNext(handler, failedEvaluatorBridge);
	}
	catch (System::Exception^ ex)
	{
		Console::WriteLine("Exception in Java_javabridge_NativeInterop_ClrSysteFailedTaskHandlerOnNext");
		Console::WriteLine(ex->Message);
		Console::WriteLine(ex->StackTrace);
	}
}

/*
 * Class:     javabridge_NativeInterop
 * Method:    ClrSystemHttpServerEventHandlerOnHttpRequest
 * Signature: (JLjavabridge/HttpServerEventBridge;Ljavabridge/InteropLogger;)V
 */
JNIEXPORT void JNICALL Java_javabridge_NativeInterop_ClrSystemHttpServerHandlerOnNext
  (JNIEnv *env , jclass cls, jlong handler, jobject jhttpServerEventBridge, jobject jlogger)
 {
	try{
		Console::WriteLine("+Java_javabridge_NativeInterop_ClrSystemHttpServerHandlerOnNext");
		HttpServerClr2Java^ httpServerClr2Java = gcnew HttpServerClr2Java(env, jhttpServerEventBridge);
		ClrSystemHandlerWrapper::Call_ClrSystemHttpServer_OnNext(handler, httpServerClr2Java);
	}
	catch (System::Exception^ ex)
	{
		Console::WriteLine("Exception in Java_javabridge_NativeInterop_ClrSystemHttpServerHandlerOnNext");
		Console::WriteLine(ex->Message);
		Console::WriteLine(ex->StackTrace);
	}
}

/*
 * Class:     javabridge_NativeInterop
 * Method:    ClrSystemHttpServerEventHandlerOnHttpRequest
 * Signature: (JLjavabridge/HttpServerEventBridge;Ljavabridge/InteropLogger;)V
 */
JNIEXPORT void JNICALL Java_javabridge_NativeInterop_ClrSystemHttpServerHandlerGetSpec
  (JNIEnv *env , jclass cls, jlong handler, jobject jhttpServerEventBridge, jobject jlogger)
 {
	try{
		Console::WriteLine("+Java_javabridge_NativeInterop_ClrSystemHttpServerHandlerGetSpec");
		HttpServerClr2Java^ httpServerClr2Java = gcnew HttpServerClr2Java(env, jhttpServerEventBridge);
		ClrSystemHandlerWrapper::Call_ClrSystemHttpServer_GetSpec(handler, httpServerClr2Java);
	}
	catch (System::Exception^ ex)
	{
		Console::WriteLine("Exception in Java_javabridge_NativeInterop_ClrSystemHttpServerHandlerGetSpec");
		Console::WriteLine(ex->Message);
		Console::WriteLine(ex->StackTrace);
	}
}

/*
 * Class:     javabridge_NativeInterop
 * Method:    ClrSystemCompletedTaskHandlerOnNext
 * Signature: (JLjavabridge/CompletedTaskBridge;Ljavabridge/InteropLogger;)V
 */
JNIEXPORT void JNICALL Java_javabridge_NativeInterop_ClrSystemCompletedTaskHandlerOnNext
  (JNIEnv *env , jclass cls, jlong handler, jobject jcompletedTask, jobject jlogger)
{
	try{
		Console::WriteLine("+Java_javabridge_NativeInterop_ClrSystemCompletedTaskHandlerOnNext");
		CompletedTaskClr2Java^ completedTaskBridge = gcnew CompletedTaskClr2Java(env, jcompletedTask);
		ClrSystemHandlerWrapper::Call_ClrSystemCompletedTask_OnNext(handler, completedTaskBridge);
	}
	catch (System::Exception^ ex)
	{
		Console::WriteLine("Exception in Java_javabridge_NativeInterop_ClrSystemCompletedTaskHandlerOnNext");
		Console::WriteLine(ex->Message);
		Console::WriteLine(ex->StackTrace);
	}
}

/*
 * Class:     javabridge_NativeInterop
 * Method:    ClrBufferedLog
 * Signature: (ILjava/lang/String;)V
 */
JNIEXPORT void JNICALL Java_javabridge_NativeInterop_ClrBufferedLog
  (JNIEnv *env, jclass cls, jint logLevel, jstring message)
{
    try {
        Logger^ logger = gcnew Logger("JavaClrBridge");
        Level level;
        switch (logLevel) {
            case 0: level = Level.Off;     break;
            case 1: level = Level.Error;   break;
            case 2: level = Level.Warning; break;
            case 3: level = Level.Info;    break;
            case 4: level = Level.Verbose; break;
        }

        String^ logMessage = ManagedStringFromJavaString(env, jfileName);
        logger::Log(level, logMessage);
    }
    catch (System::Exception^ ex) {
        Console::WriteLine("Exception in Java_javabridge_NativeInterop_ClrBufferedLog");
        Console::WriteLine(ex->Message);
        Console::WriteLine(ex->StackTrace);
    }
}


