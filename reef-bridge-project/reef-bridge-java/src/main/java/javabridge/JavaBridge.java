package javabridge;
import javabridge.NativeInterop;

/**
 * Created by beysims on 3/10/14.
 */
    public class JavaBridge {
        private final static String CPP_BRIDGE = "JavaClrBridge";

        static {
            //logger.info("Loading DLL");
            try {
                System.out.println("before load cppbridge");
                System.loadLibrary(CPP_BRIDGE);
                System.out.println("after load cppbridge");
                //logger.info("DLL is loaded from memory");
                //loadFromJar();
            } catch (UnsatisfiedLinkError e) {
                //loadFromJar();
            }
        }

        public static void main (String[] args){
            //NativeInterop.loadClrAssembly("d:\\yingda\\CSharp\\ClrHandler\\bin\\Debug\\ClrHandler.dll");
            System.out.println("before NativeInterop.createHandler1");
            long handle = NativeInterop.createHandler1 ("hello yingda0");
            byte[] value = new byte[3];
            value[0] = (byte)0xcc;
            value[1] = (byte)0x10;
            value[2] = (byte)0xee;
            NativeInterop.clrHandlerOnNext(handle, value);
            System.out.println("after loadClrAssembly");
        }

    }

