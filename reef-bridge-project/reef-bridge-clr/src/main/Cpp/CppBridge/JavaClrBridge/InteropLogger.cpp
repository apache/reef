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

#include "InteropLogger.h"

namespace Microsoft {
  namespace Reef {
    namespace Interop {
      /// currently not being used
      InteropLogger::InteropLogger (JNIEnv* env, jobject jobjectInteropLogger) {
        _env = env;
        _jobjectInteropLogger = jobjectInteropLogger;
        _jclassInteropLogger = env->GetObjectClass(jobjectInteropLogger);
        wchar_t formatBuf[1024];
        if (NULL == _jclassInteropLogger) {
          swprintf_s (formatBuf, sizeof(formatBuf) / sizeof(wchar_t), L"_jclassInteropLogger %p\n", _jclassInteropLogger);
          fwprintf (stdout, formatBuf);
          fflush (stdout);
        }
        _jmidLog  = env->GetMethodID(_jclassInteropLogger, "Log", "(ILjava/lang/String;)V");
        if (NULL == _jmidLog) {
          swprintf_s (formatBuf, sizeof(formatBuf) / sizeof(wchar_t), L"_jmidLog %p\n", _jmidLog);
          fwprintf (stdout, formatBuf);
          fflush (stdout);
        }

      }
      void InteropLogger::Log(TraceLevel traceLevel, String^ message) {
        pin_ptr<const wchar_t> wch = PtrToStringChars(message);
        jstring msg = _env->NewString((const jchar*)wch, message->Length);
        _env->CallObjectMethod(_jobjectInteropLogger, _jmidLog, (int)traceLevel, msg);
      }
    }
  }
}