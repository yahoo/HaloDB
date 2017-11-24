package amannaly;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;

/**
 * @author Arjun Mannaly
 */
public class CompactionManager extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(CompactionManager.class);

    private  final HaloDBInternal dbInternal;

    private volatile boolean isRunning = true;

    private final int intervalBetweenRunsInSeconds;

    public CompactionManager(HaloDBInternal dbInternal, int intervalBetweenRunsInSeconds) {
        super("CompactionManager");
        this.dbInternal = dbInternal;
        this.intervalBetweenRunsInSeconds = intervalBetweenRunsInSeconds;

        this.setUncaughtExceptionHandler((t, e) -> {
            logger.error("Merge thread crashed", e);
            //TODO: error handling logic.
        });
    }

    @Override
    public void run() {
        while (isRunning && !dbInternal.options.isMergeDisabled) {

            long nextRun = System.currentTimeMillis() + intervalBetweenRunsInSeconds * 1000;

            if (dbInternal.areThereEnoughFilesToMerge()) {
                Set<Integer> filesToMerge = dbInternal.getFilesToMerge();
                if (filesToMerge.size() >= dbInternal.options.mergeThresholdFileNumber) {
                    HaloDBFile mergedFile;
                    try {
                        mergedFile = dbInternal.createHaloDBFile();
                    } catch (IOException e) {
                        e.printStackTrace();
                        isRunning = false;
                        break;
                    }

                    CompactionJob job = new CompactionJob(filesToMerge, mergedFile, dbInternal);
                    job.run();
                    dbInternal.submitMergedFiles(filesToMerge);
                }
            }

            long msToSleep = Math.max(0, nextRun-System.currentTimeMillis());
            try {
                Thread.sleep(msToSleep);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void stopThread() {
        isRunning = false;
    }
}
