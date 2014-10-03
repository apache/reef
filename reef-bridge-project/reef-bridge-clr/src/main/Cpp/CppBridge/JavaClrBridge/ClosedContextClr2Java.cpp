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
        ClosedContextClr2Java::ClosedContextClr2Java(JNIEnv *env, jobject jobjectClosedContext) {
          ManagedLog::LOGGER->LogStart("ClosedContextClr2Java::ClosedContextClr2Java");

          pin_ptr<JavaVM*> pJavaVm = &_jvm;
          if (env->GetJavaVM(pJavaVm) != 0) {
            ManagedLog::LOGGER->LogError("Failed to get JavaVM", nullptr);
          }
          _jobjectClosedContext = reinterpret_cast<jobject>(env->NewGlobalRef(jobjectClosedContext));
          jclass jclassClosedContext = env->GetObjectClass (_jobjectClosedContext);

          jfieldID jidContextId = env->GetFieldID(jclassClosedContext, "contextId", "Ljava/lang/String;");
          jfieldID jidEvaluatorId = env->GetFieldID(jclassClosedContext, "evaluatorId", "Ljava/lang/String;");

          _jstringContextId = reinterpret_cast<jstring>(env->NewGlobalRef(env->GetObjectField(_jobjectClosedContext, jidContextId)));
          _jstringEvaluatorId = reinterpret_cast<jstring>(env->NewGlobalRef(env->GetObjectField(_jobjectClosedContext, jidEvaluatorId)));

          ManagedLog::LOGGER->LogStop("ClosedContextClr2Java::ClosedContextClr2Java");
        }

        IActiveContextClr2Java^ ClosedContextClr2Java::GetParentContext() {
          ManagedLog::LOGGER->LogStart("ClosedContextClr2Java::GetParentContext");

          JNIEnv *env = RetrieveEnv(_jvm);

          jclass jclassClosedContext = env->GetObjectClass(_jobjectClosedContext);
          jfieldID jidParentContext = env->GetFieldID(jclassClosedContext, "parentContext", "Lcom/microsoft/reef/javabridge/ActiveContextBridge;");
          jobject jobjectParentContext = env->GetObjectField(_jobjectClosedContext, jidParentContext);
          ManagedLog::LOGGER->LogStop("ClosedContextClr2Java::GetParentContext");

          return gcnew ActiveContextClr2Java(env, jobjectParentContext);
        }

        String^ ClosedContextClr2Java::GetId() {
          ManagedLog::LOGGER->Log("ClosedContextClr2Java::GetId");
          JNIEnv *env = RetrieveEnv(_jvm);
          return ManagedStringFromJavaString(env, _jstringContextId);
        }

        String^ ClosedContextClr2Java::GetEvaluatorId() {
          ManagedLog::LOGGER->Log("ClosedContextClr2Java::GetEvaluatorId");
          JNIEnv *env = RetrieveEnv(_jvm);
          return ManagedStringFromJavaString(env, _jstringEvaluatorId);
        }

        IEvaluatorDescriptor^ ClosedContextClr2Java::GetEvaluatorDescriptor() {
          ManagedLog::LOGGER->LogStart("ClosedContextClr2Java::GetEvaluatorDescriptor");
          return CommonUtilities::RetrieveEvaluatorDescriptor(_jobjectClosedContext, _jvm);
        }

        void ClosedContextClr2Java::OnError(String^ message) {
          ManagedLog::LOGGER->Log("ClosedContextClr2Java::OnError");
          JNIEnv *env = RetrieveEnv(_jvm);
          HandleClr2JavaError(env, message, _jobjectClosedContext);
        }
      }
    }
  }
}