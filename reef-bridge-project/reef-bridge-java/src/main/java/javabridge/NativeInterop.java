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

package javabridge;

import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.evaluator.AllocatedEvaluator;
import java.io.*;
import java.util.Date;


public class NativeInterop {

    public static native void loadClrAssembly (
            String filePath
    );

    public static  native long CallClrSystemOnStartHandler (
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

    private static final String LIB_BIN = "/";
    private static final String DLL_EXTENSION = ".dll";
    private final static String CPP_BRIDGE = "JavaClrBridge";

    static String[] managedDlls =		{
            "ClrHandler",
    };


    static {
        try {
            System.loadLibrary(CPP_BRIDGE);
            System.out.println("DLL is loaded from memory");
            loadFromJar();
        } catch (UnsatisfiedLinkError e) {
            loadFromJar();
        }
    }

    private static void loadFromJar() {
        // we need to put both DLLs to temp dir
        //System.out.println("loadFromJar 1");
        String path = "Reef_" + new Date().getTime();
        loadLib(path, CPP_BRIDGE, false);
        //logger.info("loadFromJar 2");
        File[]  files =  new File(System.getProperty("user.dir")).listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.toLowerCase().endsWith(DLL_EXTENSION);
            }
        });

        for (int i=0; i<files.length; i++)
        {
            try {
                String fileName = files[i].getName();
                String fileNameWithoutExtension =  fileName;
                if (fileName.indexOf(".") > 0) {
                    fileNameWithoutExtension = fileName.substring(0, fileName.lastIndexOf("."));
                }
                loadLib(path, fileNameWithoutExtension, true);
            } catch (Exception e) {
                System.out.println("exception " + e);
                throw e;
            }
        }

        for (int i=0; i<managedDlls.length; i++)
        {
            //System.out.println("xload " + managedDlls[i]);
            loadLib(path, managedDlls[i], true);
        }
    }

    /**
     * Puts library to temp dir and loads to memory
     */

    private static void loadLib(String path, String name, boolean copyOnly) {
        name = name + DLL_EXTENSION;
        try {
            //System.out.println("class loader [" + NativeInterop.class.toString() + "]");
            // have to use a stream
            InputStream in = NativeInterop.class.getResourceAsStream(LIB_BIN + name);
            // always write to different location
            //File fileOut = new File(System.getProperty("java.io.tmpdir") + "/" + path + LIB_BIN + name);
            String directory = System.getProperty("java.io.tmpdir") + "/" + path;
            boolean status = new File(directory).mkdir();
            File fileOut = new File(directory + LIB_BIN + name);
            //System.out.println("Writing dll to:<" + fileOut.getAbsolutePath() +">");
            OutputStream out = new FileOutputStream(fileOut);
            //System.out.println("after new FileOutputStream(fileOut)");
            if (null == in)
            {
                System.out.println(name + " cannot be found to be loaded, skipped.");
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
            //IOUtils.copy(in, out);

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
                else {
                    //System.out.println("fileOut is not NULL");
                }
                //System.out.println("fileOut.toString() " + fileOut.toString());
                NativeInterop.loadClrAssembly (fileOut.toString());
                //System.out.println("Loading DLL copyonly done");
            }
        } catch (Exception e) {
            throw new UnsatisfiedLinkError("Failed to load required DLL" +   e.getMessage());
        }

    }


}
