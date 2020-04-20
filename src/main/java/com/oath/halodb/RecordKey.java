package com.oath.halodb;

import java.util.*;

public class RecordKey {
    final byte[] key;
    public RecordKey(byte[] key) {
        this.key = key;
    }

    public byte[] getBytes() {
        return key;
    }

    @Override
    public boolean equals(Object obj) {
        // to be used in tests as we don't check if the headers are the same.

        if (this == obj) {
            return true;
        }
        if (!(obj instanceof RecordKey)) {
            return false;
        }

        RecordKey recordKey = (RecordKey)obj;
        return Arrays.equals(this.key, recordKey.getBytes());
    }


}
