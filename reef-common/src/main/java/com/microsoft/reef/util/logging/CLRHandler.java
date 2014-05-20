package com.microsoft.reef.util.logging;

import java.util.logging.*;
import java.io.PrintWriter;
import java.io.FileOutputStream;

public class CLRHandler extends Handler
{
    private PrintWriter pw;

    public CLRHandler() throws Exception
    {
        super();
        pw = new PrintWriter(new FileOutputStream("mylogfile.txt"));
    }

    @Override
    public void close() throws SecurityException
    {
        pw.close();
    }

    @Override
    public void flush()
    {
        pw.flush();
    }

    @Override
    public void publish(LogRecord record)
    {
        if (!isLoggable(record))
            return;

        pw.println(getFormatter().format(record));
    }
}
