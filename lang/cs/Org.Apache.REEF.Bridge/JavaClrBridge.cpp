/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
#include "InteropUtil.h"
#include "org_apache_reef_javabridge_NativeInterop.h"
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
using namespace Org::Apache::REEF::Driver::Bridge::Clr2java;

ref class ManagedLog {
  internal:
    static BridgeLogger^ LOGGER = BridgeLogger::GetLogger("<C++>");
};

static void MarshalErrorToJava (
  JNIEnv *env,
  jobject  jerrorInfo,
  int errorNo,
  String^ exceptionString
) {
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


// Loading Clr Assembly. Note that we do not use ManagerLogger in this method since the
// logger assembly needs to be loaded by this method before it can be used.
JNIEXPORT void JNICALL Java_org_apache_reef_javabridge_NativeInterop_loadClrAssembly
(
  JNIEnv *env,
  jclass  tobj,
  jstring jfileName) {
  try {
    Console::Write("+Java_org_apache_reef_javabridge_NativeInterop_loadClrAssembly: ");
    const wchar_t* charAsmName = UnicodeCppStringFromJavaString (env, jfileName);
    int len = env->GetStringLength(jfileName);
    wchar_t* fileName = (wchar_t* )_alloca((len + 2) * sizeof(wchar_t));
    memcpy(fileName, charAsmName, (len + 2)* sizeof(wchar_t));
    fileName[len] = 0;
    String^ asmName = ManagedStringFromJavaString(env, jfileName);
    Console::WriteLine("loading " + asmName);

    BINARY_TYPE binaryType = IsManagedBinary(fileName);
    if (binaryType == BINARY_TYPE_CLR) {
      System::Reflection::Assembly^ asm1 = Assembly::LoadFrom(asmName);
      AssemblyUtil::Add(asm1);
    }
    else if (binaryType == BINARY_TYPE_NATIVE) {
      HANDLE handle = LoadLibraryW(fileName);
    }
  }
  catch (System::Exception^ ex) {
    // We do not propagate the exception back to Java to stop driver here
    // since failure to load an assembly is not necesary devastating
    Console::Write("Exceptions in Java_org_apache_reef_javabridge_NativeInterop_loadClrAssembly");
    Console::Write(ex->Message);
    Console::Write(ex->StackTrace);
  }

}

/*
 * Class:     org_apache_reef_javabridge_NativeInterop
 * Method:    callClrSystemOnStartHandler
 * Signature: (Ljava/lang/String;Ljava/lang/String;Lorg/apache/reef/javabridge/EvaluatorRequestorBridge;)[J
 */
JNIEXPORT jlongArray JNICALL Java_org_apache_reef_javabridge_NativeInterop_callClrSystemOnStartHandler
(JNIEnv * env, jclass jclassx, jstring dateTimeString, jstring httpServerPort, jobject jevaluatorRequestorBridge) {
  try {
    ManagedLog::LOGGER->Log("+Java_org_apache_reef_javabridge_NativeInterop_callClrSystemOnStartHandler");
    DateTime dt = DateTime::Now;

	String^ strPort = ManagedStringFromJavaString(env, httpServerPort);

	EvaluatorRequestorClr2Java^ evaluatorRequestorBridge = gcnew EvaluatorRequestorClr2Java(env, jevaluatorRequestorBridge);
	array<unsigned long long>^ handlers = ClrSystemHandlerWrapper::Call_ClrSystemStartHandler_OnStart(dt, strPort, evaluatorRequestorBridge);
    return JavaLongArrayFromManagedLongArray(env, handlers);
  }
  catch (System::Exception^ ex) {
    // we cannot get error back to java here since we don't have an object to call back (although we ideally should...)
    ManagedLog::LOGGER->LogError("Exceptions in Java_org_apache_reef_javabridge_NativeInterop_callClrSystemOnStartHandler", ex);
    return NULL;
  }
}

/*
 * Class:     org_apache_reef_javabridge_NativeInterop
 * Method:    clrSystemAllocatedEvaluatorHandlerOnNext
 * Signature: (JLorg/apache/reef/javabridge/AllocatedEvaluatorBridge;Lorg/apache/reef/javabridge/InteropLogger;)V
 */
JNIEXPORT void JNICALL Java_org_apache_reef_javabridge_NativeInterop_clrSystemAllocatedEvaluatorHandlerOnNext
(JNIEnv *env, jclass cls, jlong handle, jobject jallocatedEvaluatorBridge, jobject jlogger) {
  ManagedLog::LOGGER->Log("+Java_org_apache_reef_javabridge_NativeInterop_clrSystemAllocatedEvaluatorHandlerOnNext:");
  AllocatedEvaluatorClr2Java^ allocatedEval = gcnew AllocatedEvaluatorClr2Java(env, jallocatedEvaluatorBridge);
  try {
    ClrSystemHandlerWrapper::Call_ClrSystemAllocatedEvaluatorHandler_OnNext(handle, allocatedEval);
  }
  catch (System::Exception^ ex) {
    String^ errorMessage = "Exception in Call_clrSystemAllocatedEvaluatorHandler_OnNext";
    ManagedLog::LOGGER->LogError(errorMessage, ex);
    allocatedEval -> OnError(errorMessage);
  }
}

/*
 * Class:     org_apache_reef_javabridge_NativeInterop
 * Method:    clrSystemActiveContextHandlerOnNext
 * Signature: (JLorg/apache/reef/javabridge/ActiveContextBridge;Lorg/apache/reef/javabridge/InteropLogger;)V
 */
JNIEXPORT void JNICALL Java_org_apache_reef_javabridge_NativeInterop_clrSystemActiveContextHandlerOnNext
(JNIEnv *env, jclass cls, jlong handle, jobject jactiveContextBridge, jobject jlogger) {
  ManagedLog::LOGGER->Log("+Java_org_apache_reef_javabridge_NativeInterop_clrSystemActiveContextHandlerOnNext");
  ActiveContextClr2Java^ activeContextBrdige = gcnew ActiveContextClr2Java(env, jactiveContextBridge);
  try {
    ClrSystemHandlerWrapper::Call_ClrSystemActiveContextHandler_OnNext(handle, activeContextBrdige);
  }
  catch (System::Exception^ ex) {
    String^ errorMessage = "Exception in Call_ClrSystemActiveContextHandler_OnNext";
    ManagedLog::LOGGER->LogError(errorMessage, ex);
    activeContextBrdige -> OnError(errorMessage);
  }
}

/*
 * Class:     org_apache_reef_javabridge_NativeInterop
 * Method:    ClrSystemEvaluatorRequstorHandlerOnNext
 * Signature: (JLorg/apache/reef/javabridge/EvaluatorRequstorBridge;Lorg/apache/reef/javabridge/InteropLogger;)V
 */
JNIEXPORT void JNICALL Java_org_apache_reef_javabridge_NativeInterop_ClrSystemEvaluatorRequstorHandlerOnNext
(JNIEnv *env, jclass cls, jlong handle, jobject jevaluatorRequestorBridge, jobject jlogger) {
  ManagedLog::LOGGER->Log("+Java_org_apache_reef_javabridge_NativeInterop_ClrSystemEvaluatorRequstorHandlerOnNext");
  EvaluatorRequestorClr2Java^ evaluatorRequestorBridge = gcnew EvaluatorRequestorClr2Java(env, jevaluatorRequestorBridge);
  try {
    ClrSystemHandlerWrapper::Call_ClrSystemEvaluatorRequestor_OnNext(handle, evaluatorRequestorBridge);
  }
  catch (System::Exception^ ex) {
    String^ errorMessage = "Exception in Call_ClrSystemEvaluatorRequestor_OnNext";
    ManagedLog::LOGGER->LogError(errorMessage, ex);
    evaluatorRequestorBridge -> OnError(errorMessage);
  }
}

/*
 * Class:     org_apache_reef_javabridge_NativeInterop
 * Method:    clrSystemTaskMessageHandlerOnNext
 * Signature: (J[BLorg/apache/reef/javabridge/TaskMessageBridge;Lorg/apache/reef/javabridge/InteropLogger;)V
 */
JNIEXPORT void JNICALL Java_org_apache_reef_javabridge_NativeInterop_clrSystemTaskMessageHandlerOnNext
(JNIEnv *env, jclass cls, jlong handle, jbyteArray jmessage, jobject jtaskMessageBridge, jobject jlogger) {
  ManagedLog::LOGGER->Log("+Java_org_apache_reef_javabridge_NativeInterop_clrSystemTaskMessageHandlerOnNext");
  TaskMessageClr2Java^ taskMesageBridge = gcnew TaskMessageClr2Java(env, jtaskMessageBridge);
  array<byte>^ message = ManagedByteArrayFromJavaByteArray(env, jmessage);
  try {
    ClrSystemHandlerWrapper::Call_ClrSystemTaskMessage_OnNext(handle, taskMesageBridge, message);
  }
  catch (System::Exception^ ex) {
    String^ errorMessage = "Exception in Call_ClrSystemTaskMessage_OnNext";
    ManagedLog::LOGGER->LogError(errorMessage, ex);
    taskMesageBridge -> OnError(errorMessage);
  }
}

/*
 * Class:     org_apache_reef_javabridge_NativeInterop
 * Method:    clrSystemFailedTaskHandlerOnNext
 * Signature: (JLorg/apache/reef/javabridge/FailedTaskBridge;Lorg/apache/reef/javabridge/InteropLogger;)V
 */
JNIEXPORT void JNICALL Java_org_apache_reef_javabridge_NativeInterop_clrSystemFailedTaskHandlerOnNext
(JNIEnv *env , jclass cls, jlong handler, jobject jfailedTask, jobject jlogger) {
  ManagedLog::LOGGER->Log("+Java_org_apache_reef_javabridge_NativeInterop_clrSystemFailedTaskHandlerOnNext");
  FailedTaskClr2Java^ failedTaskBridge = gcnew FailedTaskClr2Java(env, jfailedTask);
  try {
    ClrSystemHandlerWrapper::Call_ClrSystemFailedTask_OnNext(handler, failedTaskBridge);
  }
  catch (System::Exception^ ex) {
    String^ errorMessage = "Exception in Call_ClrSystemTaskMessage_OnNext";
    ManagedLog::LOGGER->LogError(errorMessage, ex);
    failedTaskBridge -> OnError(errorMessage);
  }
}

/*
 * Class:     org_apache_reef_javabridge_NativeInterop
 * Method:    clrSystemRunningTaskHandlerOnNext
 * Signature: (JLorg.apache.reef.javabridge/FailedTaskBridge;Lorg.apache.reef.javabridge/InteropLogger;)V
 */
JNIEXPORT void JNICALL Java_org_apache_reef_javabridge_NativeInterop_clrSystemRunningTaskHandlerOnNext
(JNIEnv *env , jclass cls, jlong handler, jobject jrunningTask, jobject jlogger) {
  ManagedLog::LOGGER->Log("+Java_org_apache_reef_javabridge_NativeInterop_clrSystemRunningTaskHandlerOnNext");
  RunningTaskClr2Java^ runningTaskBridge = gcnew RunningTaskClr2Java(env, jrunningTask);
  try {
    ClrSystemHandlerWrapper::Call_ClrSystemRunningTask_OnNext(handler, runningTaskBridge);
  }
  catch (System::Exception^ ex) {
    String^ errorMessage = "Exception in Call_ClrSystemRunningTask_OnNext";
    ManagedLog::LOGGER->LogError(errorMessage, ex);
    runningTaskBridge -> OnError(errorMessage);
  }
}

/*
 * Class:     org_apache_reef_javabridge_NativeInterop
 * Method:    clrSystemFailedEvaluatorHandlerOnNext
 * Signature: (JLorg/apache/reef/javabridge/FailedEvaluatorBridge;Lorg/apache/reef/javabridge/InteropLogger;)V
 */
JNIEXPORT void JNICALL Java_org_apache_reef_javabridge_NativeInterop_clrSystemFailedEvaluatorHandlerOnNext
(JNIEnv *env , jclass cls, jlong handler, jobject jfailedEvaluator, jobject jlogger) {
  ManagedLog::LOGGER->Log("+Java_org_apache_reef_javabridge_NativeInterop_clrSystemFailedEvaluatorHandlerOnNext");
  FailedEvaluatorClr2Java^ failedEvaluatorBridge = gcnew FailedEvaluatorClr2Java(env, jfailedEvaluator);
  try {
    ClrSystemHandlerWrapper::Call_ClrSystemFailedEvaluator_OnNext(handler, failedEvaluatorBridge);
  }
  catch (System::Exception^ ex) {
    String^ errorMessage = "Exception in Call_ClrSystemFailedEvaluator_OnNext";
    ManagedLog::LOGGER->LogError(errorMessage, ex);
    failedEvaluatorBridge -> OnError(errorMessage);
  }
}

/*
 * Class:     org_apache_reef_javabridge_NativeInterop
 * Method:    clrSystemHttpServerHandlerOnNext
 * Signature: (JLorg/apache/reef/javabridge/HttpServerEventBridge;Lorg/apache/reef/javabridge/InteropLogger;)V
 */
JNIEXPORT void JNICALL Java_org_apache_reef_javabridge_NativeInterop_clrSystemHttpServerHandlerOnNext
(JNIEnv *env , jclass cls, jlong handler, jobject jhttpServerEventBridge, jobject jlogger) {
  ManagedLog::LOGGER->Log("+Java_org_apache_reef_javabridge_NativeInterop_clrSystemHttpServerHandlerOnNext");
  HttpServerClr2Java^ httpServerClr2Java = gcnew HttpServerClr2Java(env, jhttpServerEventBridge);
  try {
    ClrSystemHandlerWrapper::Call_ClrSystemHttpServer_OnNext(handler, httpServerClr2Java);
  }
  catch (System::Exception^ ex) {
    String^ errorMessage = "Exception in Call_ClrSystemHttpServer_OnNext";
    ManagedLog::LOGGER->LogError(errorMessage, ex);
    httpServerClr2Java -> OnError(errorMessage);
  }
}

/*
 * Class:     org_apache_reef_javabridge_NativeInterop
 * Method:    clrSystemCompletedTaskHandlerOnNext
 * Signature: (JLorg/apache/reef/javabridge/CompletedTaskBridge;Lorg/apache/reef/javabridge/InteropLogger;)V
 */
JNIEXPORT void JNICALL Java_org_apache_reef_javabridge_NativeInterop_clrSystemCompletedTaskHandlerOnNext
(JNIEnv *env , jclass cls, jlong handler, jobject jcompletedTask, jobject jlogger) {
  ManagedLog::LOGGER->Log("+Java_org_apache_reef_javabridge_NativeInterop_clrSystemCompletedTaskHandlerOnNext");
  CompletedTaskClr2Java^ completedTaskBridge = gcnew CompletedTaskClr2Java(env, jcompletedTask);
  try {
    ClrSystemHandlerWrapper::Call_ClrSystemCompletedTask_OnNext(handler, completedTaskBridge);
  }
  catch (System::Exception^ ex) {
    String^ errorMessage = "Exception in Call_ClrSystemCompletedTask_OnNext";
    ManagedLog::LOGGER->LogError(errorMessage, ex);
    completedTaskBridge -> OnError(errorMessage);
  }
}

/*
 * Class:     org_apache_reef_javabridge_NativeInterop
 * Method:    clrBufferedLog
 * Signature: (ILjava/lang/String;)V
 */
JNIEXPORT void JNICALL Java_org_apache_reef_javabridge_NativeInterop_clrBufferedLog
(JNIEnv *env, jclass cls, jint logLevel, jstring message) {
  try {
    if (!JavaClrBridge::LoggerWrapper::initialized) {
      ManagedLog::LOGGER->Log("Initializing CLRBufferedLogHandler in java bridge...");
      JavaClrBridge::LoggerWrapper::logger->Listeners->Add(gcnew System::Diagnostics::ConsoleTraceListener());
      JavaClrBridge::LoggerWrapper::initialized = true;
    }

    System::Diagnostics::TraceEventType eventType;
    switch (logLevel) {
    case 0:
      eventType = System::Diagnostics::TraceEventType::Stop;
      break;
    case 1:
      eventType = System::Diagnostics::TraceEventType::Error;
      break;
    case 2:
      eventType = System::Diagnostics::TraceEventType::Warning;
      break;
    case 3:
      eventType = System::Diagnostics::TraceEventType::Information;
      break;
    case 4:
      eventType = System::Diagnostics::TraceEventType::Verbose;
      break;
    default:
      eventType = System::Diagnostics::TraceEventType::Information;
      break;

    }

    String^ msg = ManagedStringFromJavaString(env, message);
    msg = System::String::Concat(System::DateTime::Now, msg);
    JavaClrBridge::LoggerWrapper::logger->TraceEvent(eventType, 0, msg);
  }
  catch (System::Exception^ ex) {
    ManagedLog::LOGGER->LogError("Exception in Java_javabridge_NativeInterop_clrBufferedLog", ex);
  }
}

/*
 * Class:     org_apache_reef_javabridge_NativeInterop
 * Method:    clrSystemSuspendedTaskHandlerOnNext
 * Signature: (JLorg/apache/reef/javabridge/SuspendedTaskBridge;)V
 */
JNIEXPORT void JNICALL Java_org_apache_reef_javabridge_NativeInterop_clrSystemSuspendedTaskHandlerOnNext
(JNIEnv *env , jclass cls, jlong handler, jobject jsuspendedTask) {
  ManagedLog::LOGGER->Log("+Java_org_apache_reef_javabridge_NativeInterop_clrSystemSuspendedTaskHandlerOnNext");
  SuspendedTaskClr2Java^ suspendedTaskBridge = gcnew SuspendedTaskClr2Java(env, jsuspendedTask);
  try {
    ClrSystemHandlerWrapper::Call_ClrSystemSuspendedTask_OnNext(handler, suspendedTaskBridge);
  }
  catch (System::Exception^ ex) {
    String^ errorMessage = "Exception in Call_ClrSystemSuspendedTask_OnNext";
    ManagedLog::LOGGER->LogError(errorMessage, ex);
    suspendedTaskBridge -> OnError(errorMessage);
  }
}

/*
 * Class:     org_apache_reef_javabridge_NativeInterop
 * Method:    clrSystemCompletedEvaluatorHandlerOnNext
 * Signature: (JLorg/apache/reef/javabridge/CompletedEvaluatorBridge;)V
 */
JNIEXPORT void JNICALL Java_org_apache_reef_javabridge_NativeInterop_clrSystemCompletedEvaluatorHandlerOnNext
(JNIEnv *env , jclass cls, jlong handler, jobject jcompletedEvaluator) {
  ManagedLog::LOGGER->Log("+Java_org_apache_reef_javabridge_NativeInterop_clrSystemCompletedEvaluatorHandlerOnNext");
  CompletedEvaluatorClr2Java^ completedEvaluatorBridge = gcnew CompletedEvaluatorClr2Java(env, jcompletedEvaluator);
  try {
    ClrSystemHandlerWrapper::Call_ClrSystemCompletedEvaluator_OnNext(handler, completedEvaluatorBridge);
  }
  catch (System::Exception^ ex) {
    String^ errorMessage = "Exception in Call_ClrSystemCompletedEvaluator_OnNext";
    ManagedLog::LOGGER->LogError(errorMessage, ex);
    completedEvaluatorBridge -> OnError(errorMessage);
  }
}

/*
 * Class:     org_apache_reef_javabridge_NativeInterop
 * Method:    clrSystemClosedContextHandlerOnNext
 * Signature: (JLorg/apache/reef/javabridge/ClosedContextBridge;)V
 */
JNIEXPORT void JNICALL Java_org_apache_reef_javabridge_NativeInterop_clrSystemClosedContextHandlerOnNext
(JNIEnv *env , jclass cls, jlong handler, jobject jclosedContext) {
  ManagedLog::LOGGER->Log("+Java_org_apache_reef_javabridge_NativeInterop_clrSystemClosedContextHandlerOnNext");
  ClosedContextClr2Java^ closedContextBridge = gcnew ClosedContextClr2Java(env, jclosedContext);
  try {
    ClrSystemHandlerWrapper::Call_ClrSystemClosedContext_OnNext(handler, closedContextBridge);
  }
  catch (System::Exception^ ex) {
    String^ errorMessage = "Exception in Call_ClrSystemClosedContext_OnNext";
    ManagedLog::LOGGER->LogError(errorMessage, ex);
    closedContextBridge -> OnError(errorMessage);
  }
}

/*
 * Class:     org_apache_reef_javabridge_NativeInterop
 * Method:    clrSystemFailedContextHandlerOnNext
 * Signature: (JLorg/apache/reef/javabridge/FailedContextBridge;)V
 */
JNIEXPORT void JNICALL Java_org_apache_reef_javabridge_NativeInterop_clrSystemFailedContextHandlerOnNext
(JNIEnv *env , jclass cls, jlong handler, jobject jfailedContext) {
  ManagedLog::LOGGER->Log("+Java_org_apache_reef_javabridge_NativeInterop_clrSystemFailedContextHandlerOnNext");
  FailedContextClr2Java^ failedContextBridge = gcnew FailedContextClr2Java(env, jfailedContext);
  try {
    ClrSystemHandlerWrapper::Call_ClrSystemFailedContext_OnNext(handler, failedContextBridge);
  }
  catch (System::Exception^ ex) {
    String^ errorMessage = "Exception in Call_ClrSystemFailedContext_OnNext";
    ManagedLog::LOGGER->LogError(errorMessage, ex);
    failedContextBridge -> OnError(errorMessage);
  }
}

/*
 * Class:     org_apache_reef_javabridge_NativeInterop
 * Method:    clrSystemContextMessageHandlerOnNext
 * Signature: (JLorg/apache/reef/javabridge/ContextMessageBridge;)V
 */
JNIEXPORT void JNICALL Java_org_apache_reef_javabridge_NativeInterop_clrSystemContextMessageHandlerOnNext
(JNIEnv *env , jclass cls, jlong handler, jobject jcontextMessage) {
  ManagedLog::LOGGER->Log("+Java_org_apache_reef_javabridge_NativeInterop_clrSystemContextMessageHandlerOnNext");
  ContextMessageClr2Java^ contextMessageBridge = gcnew ContextMessageClr2Java(env, jcontextMessage);
  try {
    ClrSystemHandlerWrapper::Call_ClrSystemContextMessage_OnNext(handler, contextMessageBridge);
  }
  catch (System::Exception^ ex) {
    String^ errorMessage = "Exception in Call_ClrSystemContextMessage_OnNext";
    ManagedLog::LOGGER->LogError(errorMessage, ex);
    contextMessageBridge -> OnError(errorMessage);
  }
}

/*
* Class:     org_apache_reef_javabridge_NativeInterop
* Method:    callClrSystemOnRestartHandlerOnNext
* Signature: (Ljava/lang/String;Lorg/apache/reef/javabridge/EvaluatorRequestorBridge;Lorg/apache/reef/javabridge/DriverRestartedBridge;)[J
*/
JNIEXPORT jlongArray JNICALL Java_org_apache_reef_javabridge_NativeInterop_callClrSystemOnRestartHandlerOnNext
(JNIEnv * env, jclass jclassx, jstring httpServerPort, jobject jevaluatorRequestorBridge, jobject jdriverRestartedBridge) {
	try {
		ManagedLog::LOGGER->Log("+Java_org_apache_reef_javabridge_NativeInterop_callClrSystemOnStartHandler");
		String^ strPort = ManagedStringFromJavaString(env, httpServerPort);

		EvaluatorRequestorClr2Java^ evaluatorRequestorBridge = gcnew EvaluatorRequestorClr2Java(env, jevaluatorRequestorBridge);
		DriverRestartedClr2Java^ driverRestartedBridge = gcnew DriverRestartedClr2Java(env, jdriverRestartedBridge);
		array<unsigned long long>^ handlers = ClrSystemHandlerWrapper::Call_ClrSystemRestartHandler_OnRestart(strPort, evaluatorRequestorBridge, driverRestartedBridge);
		return JavaLongArrayFromManagedLongArray(env, handlers);
	}
	catch (System::Exception^ ex) {
		// we cannot get error back to java here since we don't have an object to call back (although we ideally should...)
		ManagedLog::LOGGER->LogError("Exceptions in Java_org_apache_reef_javabridge_NativeInterop_callClrSystemOnStartHandler", ex);
		return NULL;
	}
}

/*
 * Class:     org_apache_reef_javabridge_NativeInterop
 * Method:    clrSystemDriverRestartActiveContextHandlerOnNext
 * Signature: (JLorg/apache/reef/javabridge/ActiveContextBridge;)V
 */
JNIEXPORT void JNICALL Java_org_apache_reef_javabridge_NativeInterop_clrSystemDriverRestartActiveContextHandlerOnNext
(JNIEnv *env, jclass cls, jlong handle, jobject jactiveContextBridge) {
  ManagedLog::LOGGER->Log("+Java_org_apache_reef_javabridge_NativeInterop_clrSystemDriverRestartActiveContextHandlerOnNext");
  ActiveContextClr2Java^ activeContextBrdige = gcnew ActiveContextClr2Java(env, jactiveContextBridge);
  try {
    ClrSystemHandlerWrapper::Call_ClrSystemDriverRestartActiveContextHandler_OnNext(handle, activeContextBrdige);
  }
  catch (System::Exception^ ex) {
    String^ errorMessage = "Exception in Call_ClrSystemDriverRestartActiveContextHandler_OnNext";
    ManagedLog::LOGGER -> LogError(errorMessage, ex);
    activeContextBrdige -> OnError(errorMessage);
  }
}

/*
 * Class:     org_apache_reef_javabridge_NativeInterop
 * Method:    clrSystemDriverRestartRunningTaskHandlerOnNext
 * Signature: (JLorg/apache/reef/javabridge/RunningTaskBridge;)V
 */
JNIEXPORT void JNICALL Java_org_apache_reef_javabridge_NativeInterop_clrSystemDriverRestartRunningTaskHandlerOnNext
(JNIEnv *env , jclass cls, jlong handler, jobject jrunningTask) {
  ManagedLog::LOGGER->Log("+Java_org_apache_reef_javabridge_NativeInterop_clrSystemDriverRestartRunningTaskHandlerOnNext");
  RunningTaskClr2Java^ runningTaskBridge = gcnew RunningTaskClr2Java(env, jrunningTask);
  try {
    ClrSystemHandlerWrapper::Call_ClrSystemDriverRestartRunningTask_OnNext(handler, runningTaskBridge);
  }
  catch (System::Exception^ ex) {
    String^ errorMessage = "Exception in Call_ClrSystemDriverRestartRunningTask_OnNext";
    ManagedLog::LOGGER->LogError(errorMessage, ex);
    runningTaskBridge -> OnError(errorMessage);
  }
}

/*
* Class:     org_apache_reef_javabridge_NativeInterop
* Method:    clrSystemDriverRestartCompletedHandlerOnNext
* Signature: (JLorg/apache/reef/javabridge/generic/DriverRestartCompletedBridge;)V
*/
JNIEXPORT void JNICALL Java_org_apache_reef_javabridge_NativeInterop_clrSystemDriverRestartCompletedHandlerOnNext
(JNIEnv * env, jclass cls , jlong handler, jobject jdriverRestartCompleted) {
	ManagedLog::LOGGER->Log("+Java_org_apache_reef_javabridge_NativeInterop_clrSystemDriverRestartCompletedHandlerOnNext");
	DriverRestartCompletedClr2Java^ driverRestartCompletedBridge = gcnew DriverRestartCompletedClr2Java(env, jdriverRestartCompleted);
	try {
		ClrSystemHandlerWrapper::Call_ClrSystemDriverRestartCompleted_OnNext(handler, driverRestartCompletedBridge);
	}
	catch (System::Exception^ ex) {
		String^ errorMessage = "Exception in Call_ClrSystemDriverRestartRunningTask_OnNext";
		ManagedLog::LOGGER->LogError(errorMessage, ex);
	}
}

/*
* Class:     org_apache_reef_javabridge_NativeInterop
* Method:    clrSystemDriverRestartFailedEvaluatorHandlerOnNext
* Signature: (JLorg/apache/reef/javabridge/FailedEvaluatorBridge;Lorg/apache/reef/javabridge/InteropLogger;)V
*/
JNIEXPORT void JNICALL Java_org_apache_reef_javabridge_NativeInterop_clrSystemDriverRestartFailedEvaluatorHandlerOnNext
(JNIEnv * env, jclass cls, jlong handler, jobject jfailedEvaluator, jobject jlogger) {
	ManagedLog::LOGGER->Log("+Java_org_apache_reef_javabridge_NativeInterop_clrSystemDriverRestartFailedEvaluatorHandlerOnNext");
	FailedEvaluatorClr2Java^ failedEvaluatorBridge = gcnew FailedEvaluatorClr2Java(env, jfailedEvaluator);
	try {
		ClrSystemHandlerWrapper::Call_ClrSystemDriverRestartFailedEvaluator_OnNext(handler, failedEvaluatorBridge);
	}
	catch (System::Exception^ ex) {
		String^ errorMessage = "Exception in Call_ClrSystemDriverRestartFailedEvaluator_OnNext";
		ManagedLog::LOGGER->LogError(errorMessage, ex);
	}
}
