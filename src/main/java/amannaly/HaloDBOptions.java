package amannaly;

public class HaloDBOptions {

	//TODO; convert to private with get+set.

	// threshold of stale recordSize at which file needs to be merged.
	public double mergeThresholdPerFile = 0.75;

	public int mergeThresholdFileNumber = 4;

	public int mergeJobIntervalInSeconds = 60 * 1000;

	public long maxFileSize = 1024 * 1024; /* 1mb file recordSize */

	// used primarily for testing.
	public boolean isMergeDisabled = false;

}
