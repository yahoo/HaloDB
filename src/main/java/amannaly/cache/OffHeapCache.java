package amannaly.cache;

import org.HdrHistogram.Histogram;
import org.caffinitas.ohc.Eviction;
import org.caffinitas.ohc.OHCache;
import org.caffinitas.ohc.OHCacheBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import amannaly.KeyCache;
import amannaly.RecordMetaData;
import amannaly.Utils;

public class OffHeapCache implements KeyCache {
    private static final Logger logger = LoggerFactory.getLogger(OffHeapCache.class);

    private static final Histogram putLatencyHistogram = new Histogram(TimeUnit.SECONDS.toNanos(5), 3);

    private final OHCache<byte[], RecordMetaData> ohCache;

    public OffHeapCache() {
        this.ohCache = initializeCache();
    }

    private OHCache<byte[], RecordMetaData> initializeCache() {

        long start = System.currentTimeMillis();
        OHCache<byte[], RecordMetaData> ohCache = OHCacheBuilder.<byte[], RecordMetaData>newBuilder()
            .keySerializer(new ByteArraySerializer())
            .valueSerializer(new RecordMetaDataSerializer())
            .capacity(10l * 1024 * 1024 * 1024) // doesn't look like this is being used. probably needed for chunked.
            .segmentCount(32)
            .hashTableSize(3_125_000)  // recordSize per segment.
            .eviction(Eviction.NONE)
            .build();

        logger.info("Initialized the cache in {}", (System.currentTimeMillis() - start));

        return ohCache;
    }

    @Override
    public boolean put(byte[] key, RecordMetaData metaData) {
        long start = System.nanoTime();
        ohCache.put(key, metaData);
        putLatencyHistogram.recordValue(System.nanoTime()-start);
        return true;
    }

    @Override
    public boolean replace(byte[] key, RecordMetaData oldValue, RecordMetaData newValue) {
        return ohCache.addOrReplace(key, oldValue, newValue);
    }

    @Override
    public RecordMetaData get(byte[] key) {
        return ohCache.get(key);
    }

    @Override
    public boolean containsKey(byte[] key) {
        return ohCache.containsKey(key);
    }

    @Override
    public void printPutLatency() {
        System.out.printf("Keycache Put latency mean %f\n", putLatencyHistogram.getMean());
        System.out.printf("Keycache Put latency max %d\n", putLatencyHistogram.getMaxValue());
        System.out.printf("Keycache Put latency 99 %d\n", putLatencyHistogram.getValueAtPercentile(99.0));
        System.out.printf("Keycache Put latency total count %d\n", putLatencyHistogram.getTotalCount());

    }

    @Override
    public void printGetLatency() {

    }

    @Override
    public void printMapContents() {
        Set<Long> set = new TreeSet<>();
        ohCache.keyIterator().forEachRemaining(key -> set.add(Utils.bytesToLong(key)));
        set.forEach(key -> System.out
            .printf("%d -> %d\n", key, ohCache.get(Utils.longToBytes(key)).fileId));
    }

    @Override
    public void close() {
        try {
            ohCache.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
