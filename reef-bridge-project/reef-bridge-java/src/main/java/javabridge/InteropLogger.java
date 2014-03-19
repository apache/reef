package javabridge;
import java.util.HashMap;
import java.util.logging.Logger;
import java.util.logging.Level;


/**
 * Created by beysi_000 on 3/16/14.
 */
public class InteropLogger {
    private static final Logger LOG = Logger.getLogger("InteropLogger");
    HashMap<Integer, Level> levelHashMap;
    {
        levelHashMap = new HashMap<Integer, Level>();
        levelHashMap.put(Level.OFF.intValue(), Level.OFF);
        levelHashMap.put(Level.SEVERE.intValue(), Level.SEVERE);
        levelHashMap.put(Level.WARNING.intValue(), Level.WARNING);
        levelHashMap.put(Level.INFO.intValue(), Level.INFO);

        levelHashMap.put(Level.CONFIG.intValue(), Level.CONFIG);
        levelHashMap.put(Level.FINE.intValue(), Level.FINE);
        levelHashMap.put(Level.FINER.intValue(), Level.FINER);

        levelHashMap.put(Level.FINEST.intValue(), Level.FINEST);
        levelHashMap.put(Level.ALL.intValue(), Level.ALL);
    }

    public void Log (int intLevel, String message){
        if (levelHashMap.containsKey(intLevel)){
            Level level = levelHashMap.get(intLevel);
            LOG.log(level, message);
        }
        else
        {
            LOG.log(Level.WARNING, "Level " + intLevel + " is not a valid Log level");
            LOG.log(Level.WARNING, message);
        }

    }
}
