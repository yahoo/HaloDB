package amannaly;

import sun.misc.SharedSecrets;
import sun.misc.VM;

public class MemoryMonitor {

    public static void writeUsedDirectMemoryToStdOut()
    {
        long directMemUsed =
            SharedSecrets.getJavaNioAccess().getDirectBufferPool().getMemoryUsed();

        long directMemCapacity =
            SharedSecrets.getJavaNioAccess().getDirectBufferPool().getTotalCapacity();

        long maxDirectMemory = VM.maxDirectMemory();

        System.out.printf("Direct memory => max: %d, current limit: %d, current used: %d\n",
                          maxDirectMemory, directMemCapacity, directMemUsed);
    }
}
