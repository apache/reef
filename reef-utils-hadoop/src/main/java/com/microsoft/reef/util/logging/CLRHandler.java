package com.microsoft.reef.util.logging;

import java.util.logging.*;
import java.io.PrintWriter;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import javax.inject.Inject;
import com.microsoft.reef.javabridge.*;


public class CLRHandler extends Handler
{
    private SimpleFormatter formatter;

    @Inject
    public CLRHandler()
    {
        super();
        formatter = new SimpleFormatter();
    }

    @Override
    public void close() throws SecurityException
    {
    }

    @Override
    public void flush()
    {
    }

    @Override
    public void publish(LogRecord record)
    {
        if (!isLoggable(record))
            return;

        if (record == null)
            return;

        int level;
        Level recordLevel = record.getLevel();
        if (recordLevel.equals(Level.OFF)) {
            level = 0;
        }
        else if (recordLevel.equals(Level.SEVERE)) {
            level = 1;
        }
        else if (recordLevel.equals(Level.WARNING)) {
            level = 2;
        }
        else if (recordLevel.equals(Level.ALL)) {
            level = 4;
        }
        else {
            level = 3;
        }

        String msg = formatter.format(record);
        NativeInterop.ClrBufferedLog(level, msg);
    }
}
