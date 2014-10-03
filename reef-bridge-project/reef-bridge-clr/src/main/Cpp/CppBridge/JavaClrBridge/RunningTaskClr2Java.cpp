/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "Clr2JavaImpl.h"

namespace Microsoft {
  namespace Reef {
    namespace Driver {
      namespace Bridge {
        ref class ManagedLog {
          internal:
            static BridgeLogger^ LOGGER = BridgeLogger::GetLogger("<C++>");
        };
        RunningTaskClr2Java::RunningTaskClr2Java(JNIEnv *env, jobject jobjectRunningTask) {
          ManagedLog::LOGGER->LogStart("RunningTaskClr2Java::RunningTaskClr2Java");

          pin_ptr<JavaVM*> pJavaVm = &_jvm;
          if (env->GetJavaVM(pJavaVm) != 0) {
            ManagedLog::LOGGER->LogError("Failed to get JavaVM", nullptr);
          }
          _jobjectRunningTask = reinterpret_cast<jobject>(env->NewGlobalRef(jobjectRunningTask));

          jclass jclassRunningTask = env->GetObjectClass (_jobjectRunningTask);
          jmethodID jmidGetId = env->GetMethodID(jclassRunningTask, "getId", "()Ljava/lang/String;");

          _jstringId = reinterpret_cast<jstring>(env->NewGlobalRef(env -> CallObjectMethod(_jobjectRunningTask, jmidGetId)));
          ManagedLog::LOGGER->LogStop("RunningTaskClr2Java::RunningTaskClr2Java");
        }

        IActiveContextClr2Java^ RunningTaskClr2Java::GetActiveContext() {
          ManagedLog::LOGGER->LogStart("RunningTaskClr2Java::GetActiveContext");

          JNIEnv *env = RetrieveEnv(_jvm);

          jclass jclassRunningTask = env->GetObjectClass(_jobjectRunningTask);
          jfieldID jidActiveContext = env->GetFieldID(jclassRunningTask, "jactiveContext", "Lcom/microsoft/reef/javabridge/ActiveContextBridge;");
          jobject jobjectActiveContext = env->GetObjectField(_jobjectRunningTask, jidActiveContext);
          ManagedLog::LOGGER->LogStop("RunningTaskClr2Java::GetActiveContext");

          return gcnew ActiveContextClr2Java(env, jobjectActiveContext);
        }

        String^ RunningTaskClr2Java::GetId() {
          ManagedLog::LOGGER->Log("RunningTaskClr2Java::GetId");
          JNIEnv *env = RetrieveEnv(_jvm);
          return ManagedStringFromJavaString(env, _jstringId);
        }

        void RunningTaskClr2Java::Send(array<byte>^ message) {
          ManagedLog::LOGGER->LogStart("RunningTaskClr2Java::Send");
          JNIEnv *env = RetrieveEnv(_jvm);
          jclass jclassRunningTask = env->GetObjectClass(_jobjectRunningTask);
          jmethodID jmidSend = env->GetMethodID(jclassRunningTask, "send", "([B)V");


          if (jmidSend == NULL) {
            ManagedLog::LOGGER->Log("jmidSend is NULL");
            return;
          }
          env->CallObjectMethod(
            _jobjectRunningTask,
            jmidSend,
            JavaByteArrayFromManagedByteArray(env, message));
          ManagedLog::LOGGER->LogStop("RunningTaskClr2Java::Send");
        }

        void RunningTaskClr2Java::OnError(String^ message) {
          ManagedLog::LOGGER->Log("RunningTaskClr2Java::OnError");
          JNIEnv *env = RetrieveEnv(_jvm);
          HandleClr2JavaError(env, message, _jobjectRunningTask);
        }
      }
    }
  }
}