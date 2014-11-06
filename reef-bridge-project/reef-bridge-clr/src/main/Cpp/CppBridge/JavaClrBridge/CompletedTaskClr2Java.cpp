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
#include "Clr2JavaImpl.h"

namespace Microsoft {
  namespace Reef {
    namespace Driver {
      namespace Bridge {
        ref class ManagedLog {
          internal:
            static BridgeLogger^ LOGGER = BridgeLogger::GetLogger("<C++>");
        };

        CompletedTaskClr2Java::CompletedTaskClr2Java(JNIEnv *env, jobject jobjectCompletedTask) {
          ManagedLog::LOGGER->LogStart("CompletedTaskClr2Java::CompletedTaskClr2Java");
          pin_ptr<JavaVM*> pJavaVm = &_jvm;
          if (env->GetJavaVM(pJavaVm) != 0) {
            ManagedLog::LOGGER->LogError("Failed to get JavaVM", nullptr);
          }
          _jobjectCompletedTask = reinterpret_cast<jobject>(env->NewGlobalRef(jobjectCompletedTask));

          jclass jclassCompletedTask = env->GetObjectClass (_jobjectCompletedTask);
          jfieldID jidTaskId = env->GetFieldID(jclassCompletedTask, "taskId", "Ljava/lang/String;");
          _jstringId = reinterpret_cast<jstring>(env->NewGlobalRef(env->GetObjectField(_jobjectCompletedTask, jidTaskId)));
          ManagedLog::LOGGER->LogStop("CompletedTaskClr2Java::CompletedTaskClr2Java");
        }

        void CompletedTaskClr2Java::OnError(String^ message) {
          ManagedLog::LOGGER->Log("CompletedTaskClr2Java::OnError");
          JNIEnv *env = RetrieveEnv(_jvm);
          HandleClr2JavaError(env, message, _jobjectCompletedTask);
        }

        IActiveContextClr2Java^ CompletedTaskClr2Java::GetActiveContext() {
          ManagedLog::LOGGER->LogStart("CompletedTaskClr2Java::GetActiveContext");
          JNIEnv *env = RetrieveEnv(_jvm);

          jclass jclassCompletedTask = env->GetObjectClass (_jobjectCompletedTask);
          jfieldID jidActiveContext = env->GetFieldID(jclassCompletedTask, "jactiveContext", "Lorg/apache/reef/javabridge/ActiveContextBridge;");
          jobject jobjectActiveContext = env->GetObjectField(_jobjectCompletedTask, jidActiveContext);
          ManagedLog::LOGGER->LogStop("CompletedTaskClr2Java::GetActiveContext");
          return gcnew ActiveContextClr2Java(env, jobjectActiveContext);
        }

        String^ CompletedTaskClr2Java::GetId() {
          ManagedLog::LOGGER->Log("CompletedTaskClr2Java::GetId");
          JNIEnv *env = RetrieveEnv(_jvm);
          return ManagedStringFromJavaString(env, _jstringId);
        }
      }
    }
  }
}