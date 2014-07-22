/**
 * Copyright (C) 2013 Microsoft Corporation
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

package com.microsoft.reef.javabridge;

import java.io.*;
import java.util.HashMap;
import java.util.UUID;


public class NativeInterop {

    public static native void loadClrAssembly (
            String filePath
    );

    public static  native long[] CallClrSystemOnStartHandler (
            String dateTime
    );

    public static native void ClrSystemAllocatedEvaluatorHandlerOnNext (
            long handle,
            AllocatedEvaluatorBridge javaEvaluatorBridge,
            InteropLogger interopLogger
    );

    public static native void ClrSystemActiveContextHandlerOnNext (
            long handle,
            ActiveContextBridge javaActiveContextBridge,
            InteropLogger interopLogger
    );

    public static native void ClrSystemEvaluatorRequstorHandlerOnNext (
            long handle,
            EvaluatorRequestorBridge javaEvluatorRequstorBridge,
            InteropLogger interopLogger
    );

    public static native void ClrSystemTaskMessageHandlerOnNext (
            long handle,
            byte[] mesage,
            TaskMessageBridge javaTaskMessageBridge,
            InteropLogger interopLogger
    );

    public static native void ClrSystemFailedTaskHandlerOnNext (
            long handle,
            FailedTaskBridge failedTaskBridge,
            InteropLogger interopLogger
    );

    public static native void ClrSystemHttpServerHandlerOnNext(
            long handle,
            HttpServerEventBridge httpServerEventBridge,
            InteropLogger interopLogger
    );

    public static native void ClrSystemFailedEvaluatorHandlerOnNext (
            long handle,
            FailedEvaluatorBridge failedEvaluatorBridge,
            InteropLogger interopLogger
    );

    public static native void ClrSystemCompletedTaskHandlerOnNext (
            long handle,
            CompletedTaskBridge completedTaskBridge,
            InteropLogger interopLogger
    );

    public static native void ClrSystemRunningTaskHandlerOnNext (
            long handle,
            RunningTaskBridge runningTaskBridge,
            InteropLogger interopLogger
    );

    public static native void ClrBufferedLog(int level, String message);

    public static final String CLASS_HIERARCHY_FILENAME = "clrClassHierarchy.bin";
    private static final String LIB_BIN = "/";
    private static final String DLL_EXTENSION = ".dll";
    private final static String CPP_BRIDGE = "JavaClrBridge";

    private static final String tmpLoadingDirectory = System.getProperty("user.dir") + "/reef/CLRLoadingDirectory";
;

    static String[] managedDlls ={
            "ClrHandler",
            "msvcr110",
    };

    static {
      System.out.println("============== Driver Bridge initiated, loading DLLs ============== ");
      boolean status = new File(tmpLoadingDirectory).mkdir();
      loadFromJar();
      System.out.println("================== Done loading dlls for Driver  ================== \n");
    }

    public static String EvaluatorRequestorKey = "EvaluatorRequestor";
    public static String AllocatedEvaluatorKey = "AllocatedEvaluator";
    public static String ActiveContextKey = "ActiveContext";
    public static String TaskMessageKey = "TaskMessage";
    public static String FailedTaskKey = "FailedTask";
    public static String FailedEvaluatorKey = "FailedEvaluator";
    public static String HttpServerKey = "HttpServerKey";
    public static String CompletedTaskKey = "CompletedTask";
    public static String RunningTaskKey = "RunningTask";

  public static HashMap<String,Integer> Handlers = new HashMap<String, Integer>(){{
        put(EvaluatorRequestorKey, 0);
        put(AllocatedEvaluatorKey, 1);
        put(ActiveContextKey, 2);
        put(TaskMessageKey, 3);
        put(FailedTaskKey,4);
        put(FailedEvaluatorKey, 5);
        put(HttpServerKey, 6);
        put(CompletedTaskKey, 7);
        put(RunningTaskKey, 8);
    }};

    public static int nHandlers = 9;

    private static void loadFromJar() {
        // we need to put both DLLs to temp dir
        loadLib(CPP_BRIDGE, false);
        File[]  files =  new File(System.getProperty("user.dir") + "/reef/global").listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.toLowerCase().endsWith(DLL_EXTENSION);
            }
        });
      //System.out.println("Total dll files to load from " + System.getProperty("user.dir") + "  are: " + files.length );

      for (int i=0; i<files.length; i++)
        {
            try {
                String fileName = files[i].getName();
                String fileNameWithoutExtension =  fileName;
                if (fileName.indexOf(".") > 0) {
                    fileNameWithoutExtension = fileName.substring(0, fileName.lastIndexOf("."));
                }
                loadLib(fileNameWithoutExtension, true);
            } catch (Exception e) {
                System.out.println("exception " + e);
                throw e;
            }
        }

        for (int i=0; i<managedDlls.length; i++)
        {
            loadLib(managedDlls[i], true);
        }
    }

    /**
     * Puts library to temp dir and loads to memory
     */

    private static void loadLib(String name, boolean copyOnly) {
        name = name + DLL_EXTENSION;
        try {
            String path = "/ReefDriverAppDlls/" + name;

            //System.out.println("trying to load: " +  NativeInterop.class.getClass().getResource(path).getPath());

            InputStream in = NativeInterop.class.getResourceAsStream(path);
            // always write to different location
            File fileOut = new File(tmpLoadingDirectory + LIB_BIN + name);
            OutputStream out = new FileOutputStream(fileOut);
            //System.out.println("after new FileOutputStream(fileOut)");
            if (null == in)
            {
              //System.out.println("Cannot find " + name);
              return;
            }
            if (out == null)
            {
                System.out.println("** out is null");
            }

            int tmp;
            while ((tmp = in.read()) != -1) {
                out.write(tmp);
            }

            in.close();

            out.close();

            if (false == copyOnly)
            {
                //System.out.println("Loading DLL not copyonly");
                System.load(fileOut.toString());
                //System.out.println("Loading DLL not copyonly done");
            }
            else
            {
                //System.out.println("Loading DLL copyonly");
                if (null == fileOut)
                {
                    System.out.println("fileOut is NULL");
                }
                //System.out.println("fileOut.toString() " + fileOut.toString());
                NativeInterop.loadClrAssembly (fileOut.toString());
                //System.out.println("Done Loading DLL " +  fileOut.toString());
            }
        } catch (Exception e) {
            throw new UnsatisfiedLinkError("Failed to load required DLL " +  name);
        }

    }


}
