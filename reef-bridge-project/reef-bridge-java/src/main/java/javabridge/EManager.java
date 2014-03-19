package javabridge;

/**
 * Created by beysims on 3/18/14.
 */
public class EManager {
    public void submit(byte[] value)
    {
        System.out.println("submitting  value:");
        for (int i=0; i<value.length; i++)
        {
            System.out.println(value[i]);
        }
    }
    public void submit2()
    {
        System.out.println("submit2");
    }
    public void submit3(String s )
    {
        System.out.println("submit3 " + s);
    }

}
