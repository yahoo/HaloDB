package amannaly;

import amannaly.ohc.Eviction;
import amannaly.ohc.OHCache;
import amannaly.ohc.OHCacheBuilder;
import com.google.common.primitives.Ints;
import org.HdrHistogram.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

/**
 * @author Arjun Mannaly
 */
class OffHeapCache implements KeyCache {
    private static final Logger logger = LoggerFactory.getLogger(OffHeapCache.class);

    private static final Histogram putLatencyHistogram = new Histogram(TimeUnit.SECONDS.toNanos(5), 3);

    private final OHCache<byte[], RecordMetaDataForCache> ohCache;

    OffHeapCache(int numberOfKeys) {
        this.ohCache = initializeCache(numberOfKeys);
    }

    private OHCache<byte[], RecordMetaDataForCache> initializeCache(int numberOfKeys) {

        int noOfSegments = Ints.checkedCast(Utils.roundUpToPowerOf2(Runtime.getRuntime().availableProcessors() * 2));
        int hashTableSize = Ints.checkedCast(Utils.roundUpToPowerOf2(numberOfKeys / noOfSegments));

        long start = System.currentTimeMillis();
        OHCache<byte[], RecordMetaDataForCache> ohCache = OHCacheBuilder.<byte[], RecordMetaDataForCache>newBuilder()
            .keySerializer(new ByteArraySerializer())
            .valueSerializer(new RecordMetaDataSerializer())
            .capacity(Long.MAX_VALUE)
            .segmentCount(noOfSegments)
            .hashTableSize(hashTableSize)  // recordSize per segment.
            .fixedValueSize(RecordMetaDataForCache.SERIALIZED_SIZE)
            .loadFactor(1)   // to make sure that we don't rehash.
            .build();

        logger.info("Initialized the cache in {}", (System.currentTimeMillis() - start));

        return ohCache;
    }

    @Override
    public boolean put(byte[] key, RecordMetaDataForCache metaData) {
        long start = System.nanoTime();
        ohCache.put(key, metaData);
        putLatencyHistogram.recordValue(System.nanoTime() - start);
        return true;
    }

    @Override
    public boolean remove(byte[] key) {
        return ohCache.remove(key);
    }

    @Override
    public boolean replace(byte[] key, RecordMetaDataForCache oldValue, RecordMetaDataForCache newValue) {
        return ohCache.addOrReplace(key, oldValue, newValue);
    }

    @Override
    public RecordMetaDataForCache get(byte[] key) {
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
            .printf("%d -> %d\n", key, ohCache.get(Utils.longToBytes(key)).getFileId()));
    }

    @Override
    public void close() {
        try {
            ohCache.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public long size() {
        return ohCache.size();
    }

    //TODO: remove only for testing.
    @Override
    public String stats() {
        return ohCache.stats().toString();
    }
}
