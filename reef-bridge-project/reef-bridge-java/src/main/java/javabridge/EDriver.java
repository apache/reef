package javabridge;

/**
 * Created by beysims on 3/18/14.
 */
public class EDriver {
    EManager eManager = new EManager();
    public void submitFromEmanager(byte[] bytes)
    {
        eManager.submit(bytes);
    }
}
