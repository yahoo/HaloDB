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

/*
 * Modified for HaloDB.
 */

package amannaly.cache;

import amannaly.cache.histo.EstimatedHistogram;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Map;

public interface OHCache<K, V> extends Closeable
{
    long USE_DEFAULT_EXPIRE_AT = -1L;
    long NEVER_EXPIRE = Long.MAX_VALUE;

    /**
     * Same as {@link #put(Object, Object, long)} but uses the configured default TTL, if any.
     * @param key      key of the entry to be added. Must not be {@code null}.
     * @param value    value of the entry to be added. Must not be {@code null}.
     * @return {@code true}, if the entry has been added, {@code false} otherwise
     */
    boolean put(K key, V value);

    /**
     * Adds the key/value.
     * If the entry size of key/value exceeds the configured maximum entry length, any previously existing entry
     * for the key is removed.
     * @param key      key of the entry to be added. Must not be {@code null}.
     * @param value    value of the entry to be added. Must not be {@code null}.
     * @param expireAt timestamp in milliseconds since "epoch" (like {@link System#currentTimeMillis() System.currentTimeMillis()})
     *                 when the entry shall expire. Pass {@link #USE_DEFAULT_EXPIRE_AT} for the configured default
     *                 time-to-live or {@link #NEVER_EXPIRE} to let it never expire.
     * @return {@code true}, if the entry has been added, {@code false} otherwise
     */
    boolean put(K key, V value, long expireAt);

    /**
     * Adds key/value if either the key is not present and {@code old} is null or the existing value matches parameter {@code old}.
     *
     * @param key      key of the entry to be added or replaced. Must not be {@code null}.
     * @param old      if the entry exists, it's serialized value is compared to the serialized value of {@code old}
     *                 and only replaced, if it matches.
     * @param value    value of the entry to be added. Must not be {@code null}.
     * @return {@code true} on success or {@code false} if the existing value does not matcuh {@code old}
     */
    boolean addOrReplace(K key, V old, V value);

    /**
     * Same as {@link #putIfAbsent(Object, Object, long)} but uses the configured default TTL, if any.
     *
     * @param key      key of the entry to be added. Must not be {@code null}.
     * @param value    value of the entry to be added. Must not be {@code null}.
     * @return {@code true} on success or {@code false} if the key is already present.
     */
    boolean putIfAbsent(K key, V value);

    /**
     * Adds the key/value if the key is not present.
     * If the entry size of key/value exceeds the configured maximum entry length, any previously existing entry
     * for the key is removed.
     *
     * @param key      key of the entry to be added. Must not be {@code null}.
     * @param value    value of the entry to be added. Must not be {@code null}.
     * @param expireAt timestamp in milliseconds since "epoch" (like {@link System#currentTimeMillis() System.currentTimeMillis()})
     *                 when the entry shall expire. Pass {@link #USE_DEFAULT_EXPIRE_AT} for the configured default
     *                 time-to-live or {@link #NEVER_EXPIRE} to let it never expire.
     * @return {@code true} on success or {@code false} if the key is already present.
     */
    boolean putIfAbsent(K key, V value, long expireAt);

    /**
     * This is effectively a shortcut to add all entries in the given map {@code m}.
     *
     * @param m entries to be added
     */
    void putAll(Map<? extends K, ? extends V> m);

    /**
     * Remove a single entry for the given key.
     *
     * @param key key of the entry to be removed. Must not be {@code null}.
     * @return {@code true}, if the entry has been removed, {@code false} otherwise
     */
    boolean remove(K key);

    /**
     * This is effectively a shortcut to remove the entries for all keys given in the iterable {@code keys}.
     *
     * @param keys keys to be removed
     */
    void removeAll(Iterable<K> keys);

    /**
     * Removes all entries from the cache.
     */
    void clear();

    /**
     * Get the value for a given key.
     *
     * @param key      key of the entry to be retrieved. Must not be {@code null}.
     * @return either the non-{@code null} value or {@code null} if no entry for the requested key exists
     */
    V get(K key);

    /**
     * Checks whether an entry for a given key exists.
     * Usually, this is more efficient than testing for {@code null} via {@link #get(Object)}.
     *
     * @param key      key of the entry to be retrieved. Must not be {@code null}.
     * @return either {@code true} if an entry for the given key exists or {@code false} if no entry for the requested key exists
     */
    boolean containsKey(K key);

    /**
     * Returns a closeable byte buffer.
     * You must close the returned {@link DirectValueAccess} instance after use.
     * After closing, you must not call any of the methods of the {@link ByteBuffer}
     * returned by {@link DirectValueAccess#buffer()}.
     *
     * @return reference-counted byte buffer or {@code null} if key does not exist.
     */
    DirectValueAccess getDirect(K key);

    /**
     * Like {@link OHCache#getDirect(Object)}, but allows skipping the update of LRU stats when {@code updateLRU}
     * is {@code false}.
     *
     * @return reference-counted byte buffer or {@code null} if key does not exist.
     */
    DirectValueAccess getDirect(K key, boolean updateLRU);


    // iterators

    /**
     * Builds an iterator over all keys returning deserialized objects.
     * You must call {@code close()} on the returned iterator.
     * <p>
     *     Note: During a rehash, the implementation might return keys twice or not at all.
     * </p>
     */
    CloseableIterator<K> keyIterator();

    /**
     * Builds an iterator over all keys returning direct byte buffers.
     * Do not use a returned {@code ByteBuffer} after calling any method on the iterator.
     * You must call {@code close()} on the returned iterator.
     * <p>
     *     Note: During a rehash, the implementation might return keys twice or not at all.
     * </p>
     */
    CloseableIterator<ByteBuffer> keyBufferIterator();

    // statistics / information

    void resetStatistics();

    long size();

    int[] hashTableSizes();

    long[] perSegmentSizes();

    EstimatedHistogram getBucketHistogram();

    int segments();

    long capacity();

    long memUsed();

    long freeCapacity();

    float loadFactor();

    OHCacheStats stats();

    /**
     * Modify the cache's capacity.
     * Lowering the capacity will not immediately remove any entry nor will it immediately free allocated (off heap) memory.
     * <p>
     * Future operations will even allocate in flight, temporary memory - i.e. setting capacity to 0 does not
     * disable the cache, it will continue to work but cannot add more data.
     * </p>
     */
    void setCapacity(long capacity);
}
