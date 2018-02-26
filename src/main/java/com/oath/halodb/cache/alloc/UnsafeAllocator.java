/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

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
package com.oath.halodb.cache.alloc;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

public final class UnsafeAllocator implements IAllocator
{
    static final Unsafe unsafe;

    static
    {
        try
        {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe) field.get(null);
        }
        catch (Exception e)
        {
            throw new AssertionError(e);
        }
    }

    public long allocate(long size)
    {
        try
        {
            return unsafe.allocateMemory(size);
        }
        catch (OutOfMemoryError oom)
        {
            return 0L;
        }
    }

    public void free(long peer)
    {
        unsafe.freeMemory(peer);
    }

    public long getTotalAllocated()
    {
        return -1L;
    }
}
