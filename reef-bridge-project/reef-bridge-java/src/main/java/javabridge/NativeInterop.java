package javabridge;

        import com.microsoft.reef.driver.evaluator.AllocatedEvaluator;

        import java.io.File;
        import java.io.FileOutputStream;
        import java.io.InputStream;
        import java.io.OutputStream;
        import java.util.Date;

/**
 * Created by beysims on 3/6/14.
 */

public class NativeInterop {

    public static native void loadClrAssembly (
            String filePath
    );

    public static native long createHandler1 (
            InteropReturnInfo interopReturnInfo,
            InteropLogger interopLogger,
            String strConfig
    );

    public static native void clrHandlerOnNext (
            long handle,
            byte[] value
    );


    public static native void clrHandlerOnNext2 (
            InteropReturnInfo interopReturnInfo,
            InteropLogger interopLogger,
            long handle,
            byte[] value
    );
    public static  native long CallClrSystemOnStartHandler (
            String dateTime
    );

    public static native void CallClrSystemAllocatedEvaluatorHandlerOnNext (
            long handle,
            EManager eManager,
            DriverManager driverManager,
            InteropLogger interopLogger,
            byte[] value
    );

    public static native void ClrSystemAllocatedEvaluatorHandlerOnNext (
            long handle,
            AllocatedEvaluator javaEvaluator,
            String contextConfigurationStr,
            String taskConfigurationStr,
            InteropLogger interopLogger
    );


/*
    public static native String GetNextRow (
            long cookie,
            InteropLogger interopLogger,
            InteropReturnCode interopReturnCode
    );
*/

    private static final String LIB_BIN = "/";
    private final static String CPP_BRIDGE = "JavaClrBridge";

    static String[] managedDlls =		{
            "ClrHandler",
    };


    static {
        //System.out.println("Loading DLL");
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
        for (int i=0; i<managedDlls.length; i++)
        {
            //System.out.println("xload " + managedDlls[i]);
            loadLib(path, managedDlls[i], true);
        }
        //System.out.println("loadFromJar done.");
    }

    /**
     * Puts library to temp dir and loads to memory
     */

    private static void loadLib(String path, String name, boolean copyOnly) {
        name = name + ".dll";
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
                System.out.println("** in is null");
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
