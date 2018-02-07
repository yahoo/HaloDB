package amannaly;

/**
 * @author Arjun Mannaly
 */
class Utils {
    static long roundUpToPowerOf2(long number) {
        return (number > 1) ? Long.highestOneBit((number - 1) << 1) : 1;
    }

    static int getValueOffset(int recordOffset, byte[] key) {
        return recordOffset + Record.Header.HEADER_SIZE + key.length;
    }

    static int getValueSize(int recordSize, byte[] key) {
        return recordSize - Record.Header.HEADER_SIZE - key.length;
    }

    static RecordMetaDataForCache getMetaData(IndexFileEntry entry, int fileId) {
        return new RecordMetaDataForCache(fileId, Utils.getValueOffset(entry.getRecordOffset(), entry.getKey()), Utils.getValueSize(entry.getRecordSize(), entry.getKey()), entry.getSequenceNumber());
    }
}
