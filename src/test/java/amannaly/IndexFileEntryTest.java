package amannaly;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * @author Arjun Mannaly
 */
public class IndexFileEntryTest {

    @Test
    public void serializeIndexFileEntry() {
        byte[] key = TestUtils.generateRandomByteArray(8);
        int recordSize = 1024;
        long recordOffset = 10240L;
        short keySize = (short) key.length;
        long sequenceNumber = 100;
        byte flags = 0;

        IndexFileEntry entry = new IndexFileEntry(key, recordSize, recordOffset, sequenceNumber, flags);
        ByteBuffer[] buffers = entry.serialize();

        ByteBuffer header = ByteBuffer.allocate(IndexFileEntry.INDEX_FILE_HEADER_SIZE);
        header.putShort(keySize);
        header.putInt(recordSize);
        header.putLong(recordOffset);
        header.putLong(sequenceNumber);
        header.put(flags);
        header.flip();

        Assert.assertEquals(header, buffers[0]);
        Assert.assertEquals(ByteBuffer.wrap(key), buffers[1]);
    }
}
