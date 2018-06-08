/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package com.oath.halodb.cache;

import java.nio.ByteBuffer;

/**
 * Serialize and deserialize cached data using {@link ByteBuffer}
 */
public interface CacheSerializer<T>
{
    /**
     * Serialize the specified type into the specified {@code ByteBuffer} instance.
     *
     * @param value non-{@code null} object that needs to be serialized
     * @param buf   {@code ByteBuffer} into which serialization needs to happen.
     */
    void serialize(T value, ByteBuffer buf);

    /**
     * Deserialize from the specified {@code DataInput} instance.
     * <p>
     * Implementations of this method should never return {@code null}. Although there <em>might</em> be
     * no explicit runtime checks, a violation would break the contract of several API methods in
     * {@link OHCache}. For example users of {@link OHCache#get(Object)} might not be able to distinguish
     * between a non-existing entry or the "value" {@code null}. Instead, consider returning a singleton
     * replacement object.
     * </p>
     *
     * @param buf {@code ByteBuffer} from which deserialization needs to happen.
     * @return the type that was deserialized. Must not return {@code null}.
     */
    T deserialize(ByteBuffer buf);

    /**
     * Calculate the number of bytes that will be produced by {@link #serialize(Object, ByteBuffer)}
     * for given object {@code t}.
     *
     * @param value non-{@code null} object to calculate serialized size for
     * @return serialized size of {@code t}
     */
    int serializedSize(T value);
}

