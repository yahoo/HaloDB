package amannaly;

import com.google.common.util.concurrent.RateLimiter;
import com.google.protobuf.ByteString;

import org.HdrHistogram.Histogram;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class BitCaskPerformanceTest {

//    private static SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
//
//    private final static int numberOfRecords = 100_000_000;
//
//    private static volatile boolean isReadComplete = false;
//
//    private static final int readCount = 10_000_000;
//    private static final int readLimit = 5_000_000;
//
//    private static final RateLimiter writeRateLimiter = RateLimiter.create(7000);
//
//    static ByteString bigRecord = data(10*1024);
//    static ByteString smallRecord = data(512);
//
//    private static final Histogram histogram = new Histogram(TimeUnit.SECONDS.toMillis(5), 3);
//
//    public static void main(String[] args) throws Exception {
//        String fileName = args[0];
//        boolean isCreate = args.length == 2 && args[1].equals("create");
//
//        File dir = new File(fileName);
//
//        HaloDBOptions opts = new HaloDBOptions();
//        opts.maxFileSize = 1024*1024*1024;
//        opts.mergeJobIntervalInSeconds = 5;
//        opts.mergeThresholdFileNumber = 4;
//        opts.mergeThresholdPerFile = 0.75;
//
//        final HaloDB db = HaloDB.open(dir, opts);
//        System.out.println("Opened the database.");
//
//        if (isCreate) {
//            Thread create = createDB(db);
//            create.join();
//        }
//        else {
//            System.out.println("Starting write thread.");
//            Thread write = writeDb(db);
//
//            System.out.println("Starting read threads.");
//            doRead(db);
//
//            write.join();
//        }
//
//        db.close();
//    }
//
//    private static Thread createDB(HaloDB db) {
//        Thread write = new Thread(new CreateDB(db));
//        write.start();
//        return write;
//    }
//
//    private static Thread writeDb(HaloDB db) {
//        Thread write = new Thread(new WriteDB(db));
//        write.start();
//        return write;
//    }
//
//    private static void doRead(HaloDB db) throws InterruptedException {
//        Read[] reads = new Read[10];
//
//        for (int i = 0; i < reads.length; i++) {
//            reads[i] = new Read(db, i);
//            reads[i].start();
//        }
//
//        for (int i = 0; i < reads.length; i++) {
//            reads[i].join();
//        }
//
//        isReadComplete = true;
//
//        for (int i = 0; i < reads.length; i++) {
//            histogram.add(reads[i].getLatencyHistogram());
//        }
//
//        System.out.println("Max value - " + histogram.getMaxValue());
//        System.out.println("Average value - " + histogram.getMean());
//        System.out.println("95th percentile - " + histogram.getValueAtPercentile(95.0));
//        System.out.println("99th percentile - " + histogram.getValueAtPercentile(99.0));
//        System.out.println("99.9th percentile - " + histogram.getValueAtPercentile(99.9));
//    }
//
//    static class CreateDB implements Runnable {
//
//        final HaloDB db;
//
//        public CreateDB(HaloDB db) {
//            this.db = db;
//        }
//
//        @Override
//        public void run() {
//
//            long start = System.currentTimeMillis();
//            ByteString data;
//            long dataSize = 0;
//
//            for (int i = 0; i <= numberOfRecords; i++) {
//                if (i % 20 == 0) {
//                    data = bigRecord;
//                }
//                else {
//                    data = smallRecord;
//                }
//
//                try {
//                    dataSize += (long)data.size();
//                    db.put(ByteString.copyFrom(longToBytes(i)), data);
//
//                    if (i % 1_000_000 == 0) {
//                        System.out.printf("%s: Wrote %d records\n", DateFormat.getTimeInstance().format(new Date()), i);
//                    }
//
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
//
//            long end = System.currentTimeMillis();
//            long time = (end - start) / 1000;
//            System.out.println("Completed writing data in " + time);
//            System.out.printf("Write rate %d MB/sec\n", dataSize / time / 1024l / 1024l);
//        }
//    }
//
//    static class WriteDB implements Runnable {
//
//        final HaloDB db;
//
//        final Random rand = new Random();
//
//        public WriteDB(HaloDB db) {
//            this.db = db;
//        }
//
//        @Override
//        public void run() {
//
//            long start = System.currentTimeMillis();
//            ByteString data;
//            long dataSize = 0;
//            int count = 0;
//
//            while (!isReadComplete) {
//                writeRateLimiter.acquire();
//                count++;
//                long id = (long)rand.nextInt(readLimit);
//                id = id * 20;
//
//                //long id = (long)rand.nextInt(numberOfRecords);
//                dataSize += (long)bigRecord.size();
//                try {
//                    db.put(ByteString.copyFrom(longToBytes(id)), bigRecord);
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
//
//            long end = System.currentTimeMillis();
//            long time = (end - start) / 1000;
//            System.out.println("Completed writing data in " + time);
//            System.out.printf("Write rate %d MB/sec\n", dataSize / time / 1024l / 1024l);
//            System.out.printf("Write rate %d writes/sec\n", count / time);
//
//            HaloDB.printWriteLatencies();
//            db.printKeyCachePutLatencies();
//        }
//    }
//
//    static class Read extends Thread {
//        final int id;
//
//        final Random rand = new Random();
//
//        final HaloDB db;
//
//        Histogram latencyHistogram = new Histogram(TimeUnit.SECONDS.toMillis(10), 3);
//
//        public Histogram getLatencyHistogram() {
//            return latencyHistogram;
//        }
//
//        public Read(HaloDB db, int id) {
//            this.db = db;
//            this.id = id;
//        }
//
//        @Override
//        public void run() {
//
//            long sum = 0, count = 0;
//            long start = System.currentTimeMillis();
//
//            while (count < readCount) {
//                long id = (long)rand.nextInt(readLimit);
//                id = id * 20;
//                try {
//                    long s = System.currentTimeMillis();
//                    ByteString value = db.get(ByteString.copyFrom(longToBytes(id)));
//                    latencyHistogram.recordValue(System.currentTimeMillis()-s);
//                    count++;
//                    if (value == null) {
//                        System.out.println("NO value for key " +id);
//                        continue;
//                    }
//
//
//                    sum += value.size();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
//
//            long time = (System.currentTimeMillis() - start)/1000;
//
//            System.out.printf("Read: %d Reads per second: %d\n", id, count/time);
//            System.out.printf("Read: %d Reads rate: %d MB/sec\n", id, sum / time / 1024 / 1024);
//
//            System.out.printf("Read: %d Max value - %d\n", id, latencyHistogram.getMaxValue());
//            System.out.printf("Read: %d Average value - %f\n", id, latencyHistogram.getMean());
//            System.out.printf("Read: %d 95th percentile - %d\n", id, latencyHistogram.getValueAtPercentile(95.0));
//            System.out.printf("Read: %d 99th percentile - %d\n", id, latencyHistogram.getValueAtPercentile(99.0));
//            System.out.printf("Read: %d 99.9th percentile - %d\n", id, latencyHistogram.getValueAtPercentile(99.9));
//
//        }
//
//    }
//
//    public static byte[] longToBytes(long value) {
//        byte[] bytes = new byte[8];
//
//        for(int i = 0; i < bytes.length; ++i) {
//            bytes[bytes.length - 1 - i] = (byte)((int)value);
//            value >>>= 8;
//        }
//
//        return bytes;
//    }
//
//    private static ByteString data(int dataSize) {
//        byte[] b = new byte[dataSize];
//
//        for (int i = 0; i < dataSize; i++) {
//            b[i] = (byte)i;
//        }
//
//        return ByteString.copyFrom(b);
//    }
//
//    private static void rmdir(File dir) throws IOException, InterruptedException {
//        Process p = Runtime.getRuntime().exec(
//            new String[] { "rm", "-Rf", dir.getPath() });
//        p.waitFor();
//    }


}
