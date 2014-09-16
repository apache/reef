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

        TaskMessageClr2Java::TaskMessageClr2Java(JNIEnv *env, jobject jtaskMessage) {
          ManagedLog::LOGGER->LogStart("TaskMessageClr2Java::TaskMessageClr2Java");
          pin_ptr<JavaVM*> pJavaVm = &_jvm;
          if (env->GetJavaVM(pJavaVm) != 0) {
            ManagedLog::LOGGER->LogError("Failed to get JavaVM", nullptr);
          }
          _jobjectTaskMessage = reinterpret_cast<jobject>(env->NewGlobalRef(jtaskMessage));

          jclass jclassTaskMessage = env->GetObjectClass (_jobjectTaskMessage);
          jfieldID jidTaskId = env->GetFieldID(jclassTaskMessage, "taskId", "Ljava/lang/String;");
          _jstringId = reinterpret_cast<jstring>(env->NewGlobalRef(env->GetObjectField(_jobjectTaskMessage, jidTaskId)));
          ManagedLog::LOGGER->LogStop("TaskMessageClr2Java::TaskMessageClr2Java");
        }

        void TaskMessageClr2Java::OnError(String^ message) {
          ManagedLog::LOGGER->Log("TaskMessageClr2Java::OnError");
          JNIEnv *env = RetrieveEnv(_jvm);
          HandleClr2JavaError(env, message, _jobjectTaskMessage);
        }

        String^ TaskMessageClr2Java::GetId() {
          ManagedLog::LOGGER->Log("TaskMessageClr2Java::GetId");
          JNIEnv *env = RetrieveEnv(_jvm);
          return ManagedStringFromJavaString(env, _jstringId);
        }
      }
    }
  }
}