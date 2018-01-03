package amannaly;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;

/**
 * @author Arjun Mannaly
 */
public class RecordTest {

    @Test
    public void testSerializeHeader() {

        byte keySize = 8;
        int valueSize = 100;
        long sequenceNumber = 34543434343L;
        byte flag = 0;

        Record.Header header = new Record.Header(keySize, valueSize, sequenceNumber, flag);
        ByteBuffer serialized = header.serialize();

        Assert.assertEquals(keySize, serialized.get(Record.Header.KEY_SIZE_OFFSET));
        Assert.assertEquals(valueSize, serialized.getInt(Record.Header.VALUE_SIZE_OFFSET));
        Assert.assertEquals(sequenceNumber, serialized.getLong(Record.Header.SEQUENCE_NUMBER_OFFSET));
        Assert.assertEquals(flag, serialized.get(Record.Header.FLAGS_OFFSET));
    }

    @Test
    public void testDeserialize() {

        byte keySize = 8;
        int valueSize = 100;
        long sequenceNumber = 34543434343L;
        byte flag = 0;

        ByteBuffer buffer = ByteBuffer.allocate(Record.Header.HEADER_SIZE);
        buffer.put(keySize);
        buffer.putInt(valueSize);
        buffer.putLong(sequenceNumber);
        buffer.put(flag);
        buffer.flip();

        Record.Header header = Record.Header.deserialize(buffer);

        Assert.assertEquals(keySize, header.getKeySize());
        Assert.assertEquals(valueSize, header.getValueSize());
        Assert.assertEquals(sequenceNumber, header.getSequenceNumber());
        Assert.assertEquals(flag, header.getFlags());
        Assert.assertEquals(keySize + valueSize + Record.Header.HEADER_SIZE, header.getRecordSize());
    }

    @Test
    public void testTombstone() {
        Record record = new Record(TestUtils.generateRandomByteArray(), Record.TOMBSTONE_VALUE);
        record.markAsTombStone();

        Assert.assertEquals((byte)1, record.getFlags());
        Assert.assertTrue(record.isTombStone());
    }

    @Test
    public void testSerializeRecord() {
        byte[] key = TestUtils.generateRandomByteArray();
        byte[] value = TestUtils.generateRandomByteArray();
        long sequenceNumber = 192;

        Record record = new Record(key, value);
        record.setSequenceNumber(sequenceNumber);

        ByteBuffer[] buffers = record.serialize();

        Record.Header header = new Record.Header((byte)key.length, value.length, sequenceNumber, (byte)0);
        ByteBuffer headerBuf = header.serialize();

        Assert.assertEquals(headerBuf, buffers[0]);
        Assert.assertEquals(ByteBuffer.wrap(key), buffers[1]);
        Assert.assertEquals(ByteBuffer.wrap(value), buffers[2]);
    }
}
