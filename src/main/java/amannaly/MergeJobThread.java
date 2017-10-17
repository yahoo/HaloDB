package amannaly;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;

public class MergeJobThread extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(MergeJobThread.class);

    private  final HaloDBInternal dbInternal;

    private volatile boolean isRunning = true;

    private final int intervalBetweenRunsInSeconds;

    public MergeJobThread(HaloDBInternal dbInternal, int intervalBetweenRunsInSeconds) {
        super("MergeJobThread");
        this.dbInternal = dbInternal;
        this.intervalBetweenRunsInSeconds = intervalBetweenRunsInSeconds;

        this.setUncaughtExceptionHandler((t, e) -> {
            logger.error("Merge thread crashed", e);
            //TODO: error handling logic.
        });
    }

    //TODO: make sure that the file is not being written to.
    @Override
    public void run() {
        while (isRunning && !dbInternal.options.isMergeDisabled) {
            //System.out.println("Running Merge thread.");
            //dbInternal.printStaleFileStatus();

            long nextRun = System.currentTimeMillis() + intervalBetweenRunsInSeconds * 1000;

            if (dbInternal.areThereEnoughFilesToMerge()) {
                Set<Integer> filesToMerge = dbInternal.getFilesToMerge();
                HaloDBFile mergedFile = null;
                try {
                    mergedFile = dbInternal.createHaloDBFile();
                } catch (IOException e) {
                    e.printStackTrace();
                    isRunning = false;
                    break;
                }

                MergeJob job = new MergeJob(filesToMerge, mergedFile, dbInternal);
                job.merge();
                dbInternal.submitMergedFiles(filesToMerge);
            }
            else {
                logger.info("Not enough files to merge. Skipping merge run. ");
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
