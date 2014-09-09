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

using namespace JavaClrBridge;

namespace Microsoft {
  namespace Reef {
    namespace Driver {
      namespace Bridge {
        ref class ManagedLog {
          internal:
            static BridgeLogger^ LOGGER = BridgeLogger::GetLogger("<C++>");
        };

        FailedEvaluatorClr2Java::FailedEvaluatorClr2Java(JNIEnv *env, jobject jobjectFailedEvaluator) {
          ManagedLog::LOGGER->LogStart("FailedEvaluatorClr2Java::FailedEvaluatorClr2Java");
          pin_ptr<JavaVM*> pJavaVm = &_jvm;
          if (env->GetJavaVM(pJavaVm) != 0) {
            ManagedLog::LOGGER->LogError("Failed to get JavaVM", nullptr);
          }
          _jobjectFailedEvaluator = reinterpret_cast<jobject>(env->NewGlobalRef(jobjectFailedEvaluator));

          jclass jclassFailedEvaluator = env->GetObjectClass(_jobjectFailedEvaluator);
          jfieldID jidEvaluatorId = env->GetFieldID(jclassFailedEvaluator, "evaluatorId", "Ljava/lang/String;");
          _jstringId = reinterpret_cast<jstring>(env->NewGlobalRef(env->GetObjectField(_jobjectFailedEvaluator, jidEvaluatorId)));
          ManagedLog::LOGGER->LogStop("FailedEvaluatorClr2Java::FailedEvaluatorClr2Java");
        }

        IEvaluatorRequestorClr2Java^ FailedEvaluatorClr2Java::GetEvaluatorRequestor() {
          ManagedLog::LOGGER->LogStart("FailedEvaluatorClr2Java::GetEvaluatorRequestor");
          JNIEnv *env = RetrieveEnv(_jvm);

          jclass jclassFailedEvaluator = env->GetObjectClass(_jobjectFailedEvaluator);
          jfieldID jidEvaluatorRequestor = env->GetFieldID(jclassFailedEvaluator, "evaluatorRequestorBridge", "Lcom/microsoft/reef/javabridge/EvaluatorRequestorBridge;");
          jobject jobjectEvaluatorRequestor = env->GetObjectField(_jobjectFailedEvaluator, jidEvaluatorRequestor);
          ManagedLog::LOGGER->LogStop("FailedEvaluatorClr2Java::GetEvaluatorRequestor");
          return gcnew EvaluatorRequestorClr2Java(env, jobjectEvaluatorRequestor);
        }

        String^ FailedEvaluatorClr2Java::GetId() {
          ManagedLog::LOGGER->Log("FailedEvaluatorClr2Java::GetId");

          JNIEnv *env = RetrieveEnv(_jvm);
          return ManagedStringFromJavaString(env, _jstringId);
        }

        void FailedEvaluatorClr2Java::OnError(String^ message) {
          ManagedLog::LOGGER->Log("FailedEvaluatorClr2Java::OnError");
          JNIEnv *env = RetrieveEnv(_jvm);
          HandleClr2JavaError(env, message, _jobjectFailedEvaluator);
        }
      }
    }
  }
}