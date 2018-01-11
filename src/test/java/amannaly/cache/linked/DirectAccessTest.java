/*
 *      Copyright (C) 2014 Robert Stupp, Koeln, Germany, robert-stupp.de
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package amannaly.cache.linked;

import amannaly.ByteArraySerializer;
import com.google.common.primitives.Longs;
import amannaly.cache.DirectValueAccess;
import amannaly.cache.OHCache;
import amannaly.cache.OHCacheBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.nio.BufferUnderflowException;
import java.nio.ReadOnlyBufferException;
import java.util.ArrayList;
import java.util.List;

public class DirectAccessTest
{
    @AfterMethod(alwaysRun = true)
    public void deinit()
    {
        Uns.clearUnsDebugForTest();
    }

    @Test
    public void testDirectPutGet() throws Exception
    {
        try (OHCache<byte[], byte[]> cache = OHCacheBuilder.<byte[], byte[]>newBuilder()
                                                            .keySerializer(new ByteArraySerializer())
                                                            .valueSerializer(new ByteArraySerializer())
                                                            .capacity(64 * 1024 * 1024)
                                                            .fixedValueSize(32)
                                                            .build())
        {

            List<TestUtils.KeyValuePair> data = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                data.add(new TestUtils.KeyValuePair(Longs.toByteArray(i), TestUtils.randomBytes(32)));
            }
            data.forEach(kv -> cache.put(kv.key, kv.value));

            data.forEach(kv -> {

            });

            for (int i = 0; i < 100; i++)
            {
                byte[] key = data.get(i).key;
                byte[] value = data.get(i).value;
                Assert.assertTrue(cache.containsKey(key));
                try (DirectValueAccess direct = cache.getDirect(key))
                {
                    Assert.assertEquals(direct.buffer().capacity(), value.length);
                    Assert.assertEquals(direct.buffer().limit(), value.length);

                    byte[] actual = new byte[direct.buffer().remaining()];
                    direct.buffer().get(actual);
                    Assert.assertEquals(actual, value);

                    try
                    {
                        // read only buffer should fail.
                        direct.buffer().get();
                        Assert.fail();
                    }
                    catch (BufferUnderflowException e)
                    {
                        // fine
                    }

                    try
                    {
                        direct.buffer().put(0, (byte) 0);
                        Assert.fail();
                    }
                    catch (ReadOnlyBufferException e)
                    {
                        // fine
                    }
                }
            }
        }
    }
}
